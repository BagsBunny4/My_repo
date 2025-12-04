import os
import json
import asyncio
import uuid
import calendar
import traceback
import time
from datetime import datetime, date
import pytz

# Сторонние библиотеки
import ydb
import ydb.iam
from google.oauth2 import service_account
from googleapiclient.discovery import build
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import WebAppInfo, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.web_app import safe_parse_webapp_init_data

# --- КОНФИГУРАЦИЯ ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
YDB_ENDPOINT = os.getenv("YDB_ENDPOINT")
YDB_DATABASE = os.getenv("YDB_DATABASE")
SERVICE_ACCOUNT_EMAIL = os.getenv("SA_EMAIL")
WEB_APP_URL = os.getenv("WEB_APP_URL", "https://d4e123-finance-app.website.yandexcloud.net/") 

TRANSACTIONS_SHEET_NAME = "Транзакции"
BUDGET_SHEET_NAME = "Бюджет"
SUBSCRIPTIONS_SHEET_NAME = "Подписки"
DEBTS_SHEET_NAME = "Долги"
WALLETS_SHEET_NAME = "Кошельки" 
MOSCOW_TIMEZONE = pytz.timezone('Europe/Moscow')

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ---
YDB_DRIVER = None
YDB_POOL = None
SHEETS_SERVICE = None
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Кэш в оперативной памяти (живет пока контейнер функции "горячий")
RAM_CACHE = {}
CACHE_TTL = 60  # Время жизни кэша в секундах

# --- УТИЛИТЫ КЭШИРОВАНИЯ ---

def get_cache_key(spreadsheet_id, action, payload):
    payload_str = json.dumps(payload, sort_keys=True)
    return f"{spreadsheet_id}:{action}:{payload_str}"

def get_from_cache(key):
    if key in RAM_CACHE:
        entry = RAM_CACHE[key]
        if time.time() < entry['expire']:
            print(f"[LOG] Cache HIT for key: {key}")
            return entry['data']
        else:
            print(f"[LOG] Cache EXPIRED for key: {key}")
            del RAM_CACHE[key]
    return None

def save_to_cache(key, data, ttl=None):
    RAM_CACHE[key] = {
        'data': data,
        'expire': time.time() + (ttl if ttl is not None else CACHE_TTL)
    }

def clear_user_cache(spreadsheet_id):
    print(f"[LOG] Clearing cache for spreadsheet: {spreadsheet_id}")
    keys_to_delete = [k for k in RAM_CACHE if k.startswith(spreadsheet_id)]
    for k in keys_to_delete:
        del RAM_CACHE[k]

# --- РАБОТА С YDB (БАЗА ДАННЫХ) ---

def get_ydb_driver():
    global YDB_DRIVER
    if YDB_DRIVER is None:
        try:
            print("[LOG] Initializing YDB Driver...")
            credentials = ydb.iam.MetadataUrlCredentials()
            driver_config = ydb.DriverConfig(
                endpoint=YDB_ENDPOINT, 
                database=YDB_DATABASE, 
                credentials=credentials
            )
            YDB_DRIVER = ydb.Driver(driver_config)
            YDB_DRIVER.wait(timeout=5, fail_fast=True)
            print("[LOG] YDB Driver connected.")
        except Exception as e:
            print(f"[ERROR] YDB Connection Error: {e}")
            raise e
    return YDB_DRIVER

def get_ydb_pool():
    global YDB_POOL
    if YDB_POOL is None:
        driver = get_ydb_driver()
        YDB_POOL = ydb.SessionPool(driver)
    return YDB_POOL

def execute_query(query):
    def callee(session):
        session.transaction().execute(query, commit_tx=True)
    pool = get_ydb_pool()
    pool.retry_operation_sync(callee)

def get_safe_str(text):
    if text is None:
        return ""
    return str(text).replace('"', '').replace("'", "").replace("\\", "")

def save_user_data(telegram_id, spreadsheet_id, owner_id, first_name="User"):
    print(f"[LOG] Saving user data: {telegram_id}, Owner: {owner_id}")
    tid = int(telegram_id)
    oid = int(owner_id)
    sid = get_safe_str(spreadsheet_id)
    fname = get_safe_str(first_name)
    
    query = f"""
        UPSERT INTO `users` (telegram_id, refresh_token, spreadsheet_id, owner_id, first_name)
        VALUES ({tid}, "sa_mode", "{sid}", {oid}, "{fname}");
    """
    execute_query(query)

def get_user_data(telegram_id):
    tid = int(telegram_id)
    query = f"SELECT spreadsheet_id, owner_id FROM `users` WHERE telegram_id = {tid};"
    
    def callee(session):
        return session.transaction().execute(query, commit_tx=True)
        
    pool = get_ydb_pool()
    result_sets = pool.retry_operation_sync(callee)
        
    if result_sets and result_sets[0].rows:
        row = result_sets[0].rows[0]
        o_id = row.owner_id if row.owner_id else tid
        try:
            s_id = row.spreadsheet_id.decode('utf-8') if isinstance(row.spreadsheet_id, bytes) else row.spreadsheet_id
        except:
            s_id = row.spreadsheet_id
        print(f"[LOG] User found. Spreadsheet: {s_id}")
        return {'spreadsheet_id': s_id, 'owner_id': int(o_id)}
    
    print(f"[LOG] User {tid} not found in YDB.")
    return None

def create_default_categories(telegram_id):
    tid = int(telegram_id)
    check_q = f"SELECT COUNT(*) as cnt FROM `categories` WHERE telegram_id = {tid};"
    
    def check_callee(session):
        return session.transaction().execute(check_q, commit_tx=True)
    
    pool = get_ydb_pool()
    res = pool.retry_operation_sync(check_callee)
        
    if res[0].rows[0].cnt > 0:
        return 
    
    print(f"[LOG] Creating default categories for {tid}")
    defaults = [
        ("Продукты", "expense"), 
        ("Транспорт", "expense"), 
        ("Кафе", "expense"),
        ("ЖКХ", "expense"), 
        ("Здоровье", "expense"), 
        ("Кредиты", "expense"),
        ("Зарплата", "income"),
        ("Долги", "income"),
        ("Корректировка", "income"),
        ("Перевод", "expense"),
        ("Другое", "expense")
    ]
    
    values_list = []
    for name, c_type in defaults:
        cid = str(uuid.uuid4())
        values_list.append(f'({tid}, "{cid}", "{name}", "{c_type}")')
    
    values_str = ", ".join(values_list)
    query = f"UPSERT INTO `categories` (telegram_id, category_id, category_name, category_type) VALUES {values_str};"
    execute_query(query)

# --- GOOGLE SHEETS HELPERS ---

def get_creds():
    return service_account.Credentials.from_service_account_file("key.json")

def get_sheets_service():
    global SHEETS_SERVICE
    if SHEETS_SERVICE is None:
        creds = get_creds()
        SHEETS_SERVICE = build('sheets', 'v4', credentials=creds)
    return SHEETS_SERVICE

async def setup_sheet(spreadsheet_id):
    print(f"[LOG] Setting up sheet structure for {spreadsheet_id}")
    def run():
        service = get_sheets_service()
        try:
            meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        except Exception: 
            raise Exception("Нет доступа к таблице. Проверьте email бота.")
        
        titles = [s['properties']['title'] for s in meta.get('sheets', [])]
        requests = []
        
        if TRANSACTIONS_SHEET_NAME not in titles:
            requests.append({"addSheet": {"properties": {"title": TRANSACTIONS_SHEET_NAME}}})
        if BUDGET_SHEET_NAME not in titles:
            requests.append({"addSheet": {"properties": {"title": BUDGET_SHEET_NAME}}})
        if SUBSCRIPTIONS_SHEET_NAME not in titles:
            requests.append({"addSheet": {"properties": {"title": SUBSCRIPTIONS_SHEET_NAME}}})
        if DEBTS_SHEET_NAME not in titles:
            requests.append({"addSheet": {"properties": {"title": DEBTS_SHEET_NAME}}})
        if WALLETS_SHEET_NAME not in titles:
            requests.append({"addSheet": {"properties": {"title": WALLETS_SHEET_NAME}}})
            
        if requests:
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': requests}).execute()
        
        data = [
            {'range': f"'{TRANSACTIONS_SHEET_NAME}'!A1", 'values': [["Дата", "Сумма", "Категория", "Тип", "Комментарий", "Автор", "ID", "Wallet_UUID"]]},
            {'range': f"'{BUDGET_SHEET_NAME}'!A1", 'values': [["Категория", "Лимит", "Обновлено", "Setting_Key", "Setting_Value"]]},
            {'range': f"'{SUBSCRIPTIONS_SHEET_NAME}'!A1", 'values': [["Название", "Сумма", "Категория", "День", "Последняя_оплата", "ID"]]},
            {'range': f"'{DEBTS_SHEET_NAME}'!A1", 'values': [["Название", "Тип", "Остаток", "Ставка%", "ID", "Мин.Платеж"]]},
            {'range': f"'{WALLETS_SHEET_NAME}'!A1", 'values': [["Название", "Баланс", "Тип", "is_default", "UUID"]]}
        ]
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body={'valueInputOption': 'USER_ENTERED', 'data': data}
        ).execute()
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, run)

# --- DEBT STRATEGY ENGINE (v3.7: SAFETY NET LOGIC) ---

class DebtStrategist:
    def __init__(self, debts, extra_monthly_payment=0, current_savings=0, emergency_goal=0):
        self.debts = [d for d in debts if d['type'] == 'credit' and d['amount'] > 0]
        self.extra_money = float(extra_monthly_payment)
        self.current_savings = float(current_savings)
        self.emergency_goal = float(emergency_goal)

    def calculate_cfi(self, debt):
        if debt.get('min_payment', 0) <= 0: return 9999 
        return debt['amount'] / debt['min_payment']

    def simulate_payoff(self, strategy='avalanche', one_time_payment=0):
        sim_debts = [d.copy() for d in self.debts]
        sim_savings = self.current_savings
        
        if strategy == 'avalanche':
            sim_debts.sort(key=lambda x: x['rate'], reverse=True)
        elif strategy == 'snowball':
            sim_debts.sort(key=lambda x: x['amount'])

        total_interest_paid = 0
        months = 0
        months_filling_emergency = 0

        while any(d['amount'] > 0.01 for d in sim_debts):
            months += 1
            if months > 360: break 

            for d in sim_debts:
                if d['amount'] > 0:
                    interest = d['amount'] * (d['rate'] / 100.0 / 12.0)
                    d['amount'] += interest
                    total_interest_paid += interest

            monthly_surplus = self.extra_money
            if months == 1:
                monthly_surplus += float(one_time_payment)
            
            for d in sim_debts:
                if d['amount'] > 0:
                    mp = d.get('min_payment', 0)
                    payment = min(d['amount'], mp)
                    d['amount'] -= payment
                    if mp > payment:
                        monthly_surplus += (mp - payment)
                else:
                    monthly_surplus += d.get('min_payment', 0)

            if sim_savings < self.emergency_goal:
                needed = self.emergency_goal - sim_savings
                if monthly_surplus >= needed:
                    sim_savings += needed
                    monthly_surplus -= needed
                    if months_filling_emergency == 0: months_filling_emergency = months
                else:
                    sim_savings += monthly_surplus
                    monthly_surplus = 0
                    months_filling_emergency = months

            if monthly_surplus > 0:
                for d in sim_debts:
                    if d['amount'] > 0:
                        payment = min(d['amount'], monthly_surplus)
                        d['amount'] -= payment
                        monthly_surplus -= payment
                        if monthly_surplus <= 0: break
        
        freedom_date = datetime.now()
        year = freedom_date.year + (freedom_date.month + months - 1) // 12
        month = (freedom_date.month + months - 1) % 12 + 1
        freedom_str = f"{month:02d}.{year}"

        return {
            "strategy": strategy,
            "months_to_free": months,
            "freedom_date": freedom_str,
            "total_interest": round(total_interest_paid, 2),
            "months_saved_emergency": months_filling_emergency,
            "is_emergency_first": (self.current_savings < self.emergency_goal)
        }

# --- BUSINESS LOGIC HELPERS ---

def get_default_wallet_uuid(service, spreadsheet_id):
    try:
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{WALLETS_SHEET_NAME}'!A2:E").execute()
        rows = resp.get('values', [])
        first_valid_uuid = None
        for r in rows:
            if len(r) >= 5:
                if not first_valid_uuid: first_valid_uuid = r[4]
                if str(r[3]).upper() == 'TRUE': return r[4]
        return first_valid_uuid
    except Exception as e:
        print(f"[ERROR] getting default wallet: {e}")
    return None

def update_wallet_balance(service, spreadsheet_id, wallet_uuid, delta_amount):
    print(f"[LOG] Updating wallet {wallet_uuid} by {delta_amount}")
    if not wallet_uuid or delta_amount == 0: return

    try:
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{WALLETS_SHEET_NAME}'!A2:E").execute()
        rows = resp.get('values', [])
        target_index = -1
        current_balance = 0.0
        
        for i, r in enumerate(rows):
            if len(r) >= 5 and str(r[4]).strip() == str(wallet_uuid).strip():
                target_index = i
                try: current_balance = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                except: current_balance = 0.0
                break
        
        if target_index != -1:
            new_balance = current_balance + delta_amount
            update_range = f"'{WALLETS_SHEET_NAME}'!B{target_index + 2}"
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, 
                range=update_range, 
                valueInputOption='USER_ENTERED', 
                body={'values': [[new_balance]]}
            ).execute()
            print(f"[LOG] Wallet updated. New balance: {new_balance}")
    except Exception as e:
        print(f"[ERROR] Update Wallet Balance Error: {e}")

async def check_budget_and_notify(service, spreadsheet_id, category_name, user_id, amount_added):
    try:
        ranges = [f"'{BUDGET_SHEET_NAME}'!A:B", f"'{TRANSACTIONS_SHEET_NAME}'!A2:C"]
        resp = service.spreadsheets().values().batchGet(spreadsheetId=spreadsheet_id, ranges=ranges).execute()
        b_rows = resp.get('valueRanges', [])[0].get('values', [])
        t_rows = resp.get('valueRanges', [])[1].get('values', [])
        limit = 0.0
        for r in b_rows:
            if len(r) >= 2 and r[0] == category_name:
                try: limit = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                except: pass
                break
        
        if limit <= 0: return

        now = datetime.now(MOSCOW_TIMEZONE)
        total_spent = 0.0
        for r in t_rows:
            if len(r) < 3 or r[2] != category_name: continue
            try:
                dt = datetime.strptime(r[0], '%d.%m.%Y %H:%M:%S')
                if dt.month == now.month and dt.year == now.year:
                    val = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                    if val < 0: total_spent += abs(val)
            except: continue
            
        pct = (total_spent / limit) * 100
        msg_text = ""
        spent_fmt = f"{int(total_spent):,} ₽".replace(',', ' ')
        limit_fmt = f"{int(limit):,} ₽".replace(',', ' ')
        
        if total_spent > limit:
            diff = total_spent - limit
            msg_text = (f"🚨 <b>Лимит превышен!</b>\n"
                        f"Категория: {category_name}\n"
                        f"Потрачено: {spent_fmt} из {limit_fmt}\n"
                        f"Превышение: {int(diff)} ₽")
        elif pct >= 80:
            msg_text = (f"⚠️ <b>Внимание!</b>\n"
                        f"Категория: {category_name}\n"
                        f"Вы потратили {int(pct)}% бюджета ({spent_fmt}).")
        
        if msg_text:
            await bot.send_message(chat_id=user_id, text=msg_text, parse_mode="HTML")
    except Exception as e:
        print(f"[ERROR] Notification Error: {e}")

async def process_subscriptions(service, spreadsheet_id):
    try:
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"'{SUBSCRIPTIONS_SHEET_NAME}'!A2:F").execute()
        rows = resp.get('values', [])
        if not rows: return False

        now = datetime.now(MOSCOW_TIMEZONE)
        _, last_day_of_month = calendar.monthrange(now.year, now.month)
        updates = []; new_transactions = []; has_changes = False
        default_wallet = get_default_wallet_uuid(service, spreadsheet_id)

        for i, r in enumerate(rows):
            if len(r) < 6: continue
            name, amount_str, category, day_str, last_paid_str, sub_id = r[0], r[1], r[2], r[3], r[4], r[5]
            try:
                sub_day = int(day_str)
                target_day = min(sub_day, last_day_of_month)
                last_paid_date = None
                if last_paid_str and last_paid_str != "-":
                    try: last_paid_date = datetime.strptime(last_paid_str, '%d.%m.%Y')
                    except: pass
                
                should_pay = False
                if now.day >= target_day:
                    if last_paid_date is None: should_pay = True
                    elif last_paid_date.month != now.month or last_paid_date.year != now.year: should_pay = True
                
                if should_pay:
                    amount = float(amount_str)
                    final_amt = -abs(amount)
                    trans_date_str = datetime(now.year, now.month, target_day, 10, 0, 0).strftime('%d.%m.%Y %H:%M:%S')
                    new_transactions.append([
                        trans_date_str, final_amt, category, "Расход", 
                        f"Подписка: {name}", "System", str(uuid.uuid4()), default_wallet if default_wallet else ""
                    ])
                    updates.append({ 'range': f"'{SUBSCRIPTIONS_SHEET_NAME}'!E{i+2}", 'values': [[now.strftime('%d.%m.%Y')]] })
                    has_changes = True
                    if default_wallet: update_wallet_balance(service, spreadsheet_id, default_wallet, final_amt)
            except Exception as e: continue
        
        if new_transactions:
            service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=f"'{TRANSACTIONS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': new_transactions}).execute()
        if updates:
            data = [{'range': u['range'], 'values': u['values']} for u in updates]
            service.spreadsheets().values().batchUpdate(spreadsheetId=spreadsheet_id, body={'valueInputOption': 'USER_ENTERED', 'data': data}).execute()
        return has_changes
    except Exception as e: return False

# --- TELEGRAM HANDLERS ---

@dp.message(CommandStart())
async def start_cmd(message: types.Message):
    uid = message.from_user.id
    first_name = message.from_user.first_name
    user = get_user_data(uid)
    args = message.text.split(' ')[1] if len(message.text.split(' ')) > 1 else None

    if args and args.startswith('join_'):
        try:
            owner_id = int(args.split('_')[1])
            owner_data = get_user_data(owner_id)
            if owner_data:
                save_user_data(uid, owner_data['spreadsheet_id'], owner_id, first_name)
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Открыть Финансы", web_app=WebAppInfo(url=WEB_APP_URL))]])
                await message.answer(f"✅ Вы успешно присоединились к семейному бюджету!", reply_markup=kb)
            else: await message.answer("❌ Семья не найдена.")
        except: await message.answer("❌ Некорректная ссылка.")
        return

    if user:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Открыть Финансы", web_app=WebAppInfo(url=WEB_APP_URL))]])
        await message.answer("✅ Вы уже настроены.", reply_markup=kb)
    else:
        await message.answer(
            f"👋 <b>Добро пожаловать!</b>\n\n"
            f"1. Создайте Google Таблицу.\n"
            f"2. Добавьте бота: `{SERVICE_ACCOUNT_EMAIL}` (как Редактора)\n"
            f"3. Пришлите мне ссылку на таблицу.", 
            parse_mode="HTML"
        )

@dp.message(F.text.contains("google.com"))
async def sheet_handler(message: types.Message):
    link = message.text
    try:
        if '/d/' in link: sid = link.split('/d/')[1].split('/')[0]
        else:
            await message.answer("❌ Это не ссылка на таблицу.")
            return
        msg = await message.answer("⏳ Настраиваю таблицу...")
        await setup_sheet(sid)
        save_user_data(message.from_user.id, sid, message.from_user.id, message.from_user.first_name)
        create_default_categories(message.from_user.id)
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚀 Запустить", web_app=WebAppInfo(url=WEB_APP_URL))]])
        await bot.edit_message_text(text="✅ <b>Готово!</b>", chat_id=message.chat.id, message_id=msg.message_id, parse_mode="HTML", reply_markup=kb)
    except Exception as e: await message.answer(f"❌ Ошибка: {str(e)}")

# --- MAIN API HANDLER ---

async def handler(event, context):
    print(f"[LOG] >>> NEW REQUEST. Method: {event.get('httpMethod')}")
    cors = {
        'Access-Control-Allow-Origin': '*', 
        'Access-Control-Allow-Headers': 'Content-Type', 
        'Access-Control-Allow-Methods': 'POST, GET, OPTIONS'
    }
    
    if event['httpMethod'] == 'OPTIONS': 
        return {'statusCode': 200, 'headers': cors, 'body': 'ok'}

    try:
        body_str = event.get('body', '{}')
        print(f"[LOG] Body length: {len(body_str)}")
        body = json.loads(body_str)
        
        if 'update_id' in body:
            print("[LOG] Processing Telegram Update")
            await dp.feed_update(bot=bot, update=types.Update.model_validate(body, context={"bot": bot}))
            return {'statusCode': 200, 'body': 'ok'}

        if 'action' in body:
            action = body['action']
            print(f"[LOG] Processing Action: {action}")
            try: init_data = safe_parse_webapp_init_data(BOT_TOKEN, body.get('initData'))
            except Exception as e: 
                print(f"[ERROR] Auth failed: {e}")
                return {'statusCode': 401, 'headers': cors, 'body': 'Auth Error'}
            
            uid = init_data.user.id
            payload = body.get('payload', {})
            print(f"[LOG] User: {uid}, Payload: {json.dumps(payload, ensure_ascii=False)}")
            
            if action == 'check_user':
                u = get_user_data(uid)
                if u: save_user_data(uid, u['spreadsheet_id'], u['owner_id'], init_data.user.first_name)
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps({'is_registered': bool(u), 'user_id': uid})}
            
            user_data = get_user_data(uid)
            if not user_data: 
                print("[LOG] User not found in DB")
                return {'statusCode': 403, 'headers': cors, 'body': 'User not found'}
            
            sid = user_data['spreadsheet_id']
            oid = int(user_data['owner_id'])
            print(f"[LOG] Spreadsheet ID: {sid}, Owner ID: {oid}")
            
            pool = get_ydb_pool()
            
            if action == 'update_structure':
                await setup_sheet(sid)
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}
            
            if action == 'get_categories':
                start = time.time()
                cache_key = get_cache_key(sid, 'get_categories', {})
                cached = get_from_cache(cache_key)
                if cached:
                    print(f"[PERF] get_categories took {time.time() - start:.3f} sec (cache)")
                    return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}
                
                query = f"SELECT category_id, category_name, category_type FROM `categories` WHERE telegram_id = {oid};"
                
                def callee(session):
                    return session.transaction().execute(query, commit_tx=True)
                
                res = pool.retry_operation_sync(callee)
                cats = []
                
                if res and res[0].rows:
                    for row in res[0].rows:
                        try:
                            cn = row.category_name.decode('utf-8') if isinstance(row.category_name, bytes) else row.category_name
                            ct = row.category_type.decode('utf-8') if isinstance(row.category_type, bytes) else row.category_type
                            ci = row.category_id.decode('utf-8') if isinstance(row.category_id, bytes) else row.category_id
                        except:
                            cn, ct, ci = row.category_name, row.category_type, row.category_id
                        cats.append({"id": ci, "name": cn, "type": ct})
                
                save_to_cache(cache_key, cats, ttl=300)
                print(f"[PERF] get_categories took {time.time() - start:.3f} sec")
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cats)}

            if action == 'add_category':
                query = f"""
                    UPSERT INTO `categories` (telegram_id, category_id, category_name, category_type)
                    VALUES ({oid}, "{str(uuid.uuid4())}", "{get_safe_str(payload['name'])}", "{get_safe_str(payload['type'])}");
                """
                execute_query(query)
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}
            
            if action == 'edit_category':
                query = f"""
                    UPDATE `categories` SET category_name = "{get_safe_str(payload['new_name'])}"
                    WHERE telegram_id = {oid} AND category_id = "{get_safe_str(payload['id'])}";
                """
                execute_query(query)
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}
            
            if action == 'delete_category':
                query = f"DELETE FROM `categories` WHERE telegram_id = {oid} AND category_id = \"{get_safe_str(payload['id'])}\";"
                execute_query(query)
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'get_category_stats':
                cache_key = get_cache_key(sid, 'get_category_stats', payload)
                cached = get_from_cache(cache_key)
                if cached: return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}
                
                service = get_sheets_service()
                
                try:
                    resp = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!A2:C").execute()
                    rows = resp.get('values', [])
                except: rows = []
                
                cat_name = payload['category']
                monthly_spent = {}
                
                for r in rows:
                    if len(r) < 3 or r[2] != cat_name: continue
                    try:
                        dt = datetime.strptime(r[0], '%d.%m.%Y %H:%M:%S')
                        amt = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                        if amt < 0:
                            key = f"{dt.year}-{dt.month:02d}"
                            monthly_spent[key] = monthly_spent.get(key, 0) + abs(amt)
                    except: continue
                
                history = []
                sorted_keys = sorted(monthly_spent.keys(), reverse=True)
                
                for k in sorted_keys[:3]:
                    year, month = map(int, k.split('-'))
                    label = datetime(year, month, 1).strftime('%B %Y')
                    history.append({"label": label, "amount": monthly_spent[k]})
                
                result = {"history": history}
                save_to_cache(cache_key, result)
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(result)}

            if action == 'set_budget':
                service = get_sheets_service()
                
                try:
                    resp = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'!A2:C").execute()
                    rows = resp.get('values', [])
                except: rows = []
                
                cat_name = payload['category_name']
                limit_val = float(payload['limit'])
                found_idx = -1
                
                for i, r in enumerate(rows):
                    if len(r) >= 1 and r[0] == cat_name:
                        found_idx = i
                        break
                
                if found_idx != -1:
                    service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'!B{found_idx+2}:C{found_idx+2}", valueInputOption='USER_ENTERED', body={'values': [[limit_val, datetime.now(MOSCOW_TIMEZONE).strftime('%d.%m.%Y')]]}).execute()
                else:
                    new_row = [cat_name, limit_val, datetime.now(MOSCOW_TIMEZONE).strftime('%d.%m.%Y')]
                    service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [new_row]}).execute()
                
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'get_settings':
                service = get_sheets_service()
                
                try:
                    resp = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'!D2:E").execute()
                    rows = resp.get('values', [])
                except: rows = []
                
                settings = {}
                for r in rows:
                    if len(r) >= 2:
                        settings[r[0]] = r[1]
                
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(settings)}

            if action == 'set_setting':
                service = get_sheets_service()
                
                try:
                    resp = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'!D2:E").execute()
                    rows = resp.get('values', [])
                except: rows = []
                
                key = payload['key']
                value = payload['value']
                found_idx = -1
                
                for i, r in enumerate(rows):
                    if len(r) >= 1 and r[0] == key:
                        found_idx = i
                        break
                
                if found_idx != -1:
                    service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'!E{found_idx+2}", valueInputOption='USER_ENTERED', body={'values': [[value]]}).execute()
                else:
                    new_row = [[], [], [], key, value]
                    service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{BUDGET_SHEET_NAME}'!A", valueInputOption='USER_ENTERED', body={'values': [new_row]}).execute()
                
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'get_subscriptions':
                cache_key = get_cache_key(sid, 'get_subscriptions', {})
                cached = get_from_cache(cache_key)
                if cached: return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}
                
                service = get_sheets_service()
                
                try:
                    resp = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{SUBSCRIPTIONS_SHEET_NAME}'!A2:F").execute()
                    rows = resp.get('values', [])
                except: rows = []
                
                subs = []
                for r in rows:
                    if len(r) < 6: continue
                    subs.append({"name": r[0], "amount": float(r[1]), "category": r[2], "day": int(r[3]), "last_paid": r[4], "id": r[5]})
                
                save_to_cache(cache_key, subs)
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(subs)}

            if action == 'add_subscription':
                service = get_sheets_service()
                
                new_row = [payload['name'], float(payload['amount']), payload['category'], int(payload['day']), "-", str(uuid.uuid4())]
                service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{SUBSCRIPTIONS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [new_row]}).execute()
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'delete_subscription':
                service = get_sheets_service()
                
                rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{SUBSCRIPTIONS_SHEET_NAME}'!F:F").execute().get('values', [])
                target_id = str(payload['id']).strip()
                idx = next((i for i, r in enumerate(rows) if r and r[0].strip() == target_id), -1)
                
                if idx != -1:
                    sheet_meta = service.spreadsheets().get(spreadsheetId=sid).execute()
                    sheet_id = next(s['properties']['sheetId'] for s in sheet_meta['sheets'] if s['properties']['title'] == SUBSCRIPTIONS_SHEET_NAME)
                    service.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": [{"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": idx, "endIndex": idx + 1}}}]}).execute()
                    clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'add_transaction':
                service = get_sheets_service()
                
                amount = float(payload['amount'])
                final_amount = -abs(amount) if payload['type'] == 'expense' else abs(amount)
                
                wallet_uuid = payload.get('wallet_uuid') or get_default_wallet_uuid(service, sid)
                if wallet_uuid:
                    update_wallet_balance(service, sid, wallet_uuid, final_amount)
                
                d_str = payload.get('date')
                formatted_date = (datetime.strptime(d_str, '%Y-%m-%d').strftime('%d.%m.%Y') + datetime.now(MOSCOW_TIMEZONE).strftime(' %H:%M:%S')) if d_str else datetime.now(MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M:%S')
                
                row = [formatted_date, final_amount, payload['category'], "Расход" if final_amount < 0 else "Доход", payload.get('comment', ''), init_data.user.first_name, str(uuid.uuid4()), wallet_uuid if wallet_uuid else ""]
                service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [row]}).execute()
                
                if final_amount < 0:
                    await check_budget_and_notify(service, sid, payload['category'], uid, abs(final_amount))
                
                clear_user_cache(sid)
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}
            
            if action == 'delete_transaction':
                service = get_sheets_service()
                
                rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!G:G").execute().get('values', [])
                target_id = str(payload['id']).strip()
                idx = next((i for i, r in enumerate(rows) if r and r[0].strip() == target_id), -1)
                
                if idx != -1:
                    rows_full = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!A{idx+2}:H{idx+2}").execute().get('values', [[]])[0]
                    if len(rows_full) >= 8:
                        old_amount = float(rows_full[1])
                        old_wallet_uuid = rows_full[7]
                        if old_wallet_uuid:
                            update_wallet_balance(service, sid, old_wallet_uuid, -old_amount)
                    
                    sheet_meta = service.spreadsheets().get(spreadsheetId=sid).execute()
                    sheet_id = next(s['properties']['sheetId'] for s in sheet_meta['sheets'] if s['properties']['title'] == TRANSACTIONS_SHEET_NAME)
                    service.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": [{"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": idx, "endIndex": idx + 1}}}]}).execute()
                    clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'edit_transaction':
                service = get_sheets_service()
                
                rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!G:G").execute().get('values', [])
                target_id = str(payload['id']).strip()
                idx = -1
                old_amount = 0.0
                old_wallet_uuid = None
                
                for i, r in enumerate(rows):
                    if len(r) >= 1 and r[0].strip() == target_id:
                        idx = i
                        full_row = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!A{idx+2}:H{idx+2}").execute().get('values', [[]])[0]
                        try:
                            old_amount = float(str(full_row[1]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                            if len(full_row) >= 8: old_wallet_uuid = full_row[7]
                        except: pass
                        break
                
                if idx != -1:
                    new_amount = -abs(float(payload['amount'])) if payload['type'] == 'expense' else abs(float(payload['amount']))
                    new_wallet_uuid = payload.get('wallet_uuid') or old_wallet_uuid or get_default_wallet_uuid(service, sid)
                    
                    if old_wallet_uuid: update_wallet_balance(service, sid, old_wallet_uuid, -old_amount)
                    if new_wallet_uuid: update_wallet_balance(service, sid, new_wallet_uuid, new_amount)
                    
                    d_str = payload.get('date')
                    fd = (datetime.strptime(d_str, '%Y-%m-%d').strftime('%d.%m.%Y') + datetime.now(MOSCOW_TIMEZONE).strftime(' %H:%M:%S')) if d_str else None
                    
                    range_upd = f"'{TRANSACTIONS_SHEET_NAME}'!A{idx+2}:E{idx+2}" if fd else f"'{TRANSACTIONS_SHEET_NAME}'!B{idx+2}:E{idx+2}"
                    vals = [[fd, new_amount, payload['category'], "Расход" if new_amount < 0 else "Доход", payload.get('comment', '')]] if fd else [[new_amount, payload['category'], "Расход" if new_amount < 0 else "Доход", payload.get('comment', '')]]
                    service.spreadsheets().values().update(spreadsheetId=sid, range=range_upd, valueInputOption='USER_ENTERED', body={'values': vals}).execute()
                    service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!H{idx+2}", valueInputOption='USER_ENTERED', body={'values': [[new_wallet_uuid]]}).execute()
                    clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'get_wallets':
                start = time.time()
                cache_key = get_cache_key(sid, 'get_wallets', {})
                cached = get_from_cache(cache_key)
                if cached:
                    print(f"[PERF] get_wallets took {time.time() - start:.3f} sec (cache)")
                    return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}

                service = get_sheets_service()
                
                try:
                    res = service.spreadsheets().values().batchGet(spreadsheetId=sid, ranges=[f"'{WALLETS_SHEET_NAME}'!A2:E", f"'{DEBTS_SHEET_NAME}'!A2:E"]).execute()
                    w_rows = res['valueRanges'][0].get('values', [])
                    d_rows = res['valueRanges'][1].get('values', [])
                except: w_rows = []; d_rows = []
                
                wallets = []
                total_cash = 0.0
                
                for r in w_rows:
                    if len(r) < 5: continue
                    try:
                        bal = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                        wallets.append({"name": r[0], "balance": bal, "type": r[2], "is_default": r[3].upper() == 'TRUE', "uuid": r[4]})
                        total_cash += bal
                    except: continue 
                
                total_owed_me = 0.0; total_i_owe = 0.0
                for r in d_rows:
                    if len(r) < 5: continue
                    try:
                        amt = float(str(r[2]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                        if r[1] == 'credit': total_i_owe += amt
                        elif r[1] == 'debit': total_owed_me += amt
                    except: continue
                
                result = {"wallets": wallets, "net_worth": total_cash + total_owed_me - total_i_owe}
                save_to_cache(cache_key, result, ttl=30)
                print(f"[PERF] get_wallets took {time.time() - start:.3f} sec")
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(result)}

            if action == 'manage_wallet':
                service = get_sheets_service()
                
                if payload.get('type') == 'add':
                    is_default = payload.get('is_default', False)
                    rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{WALLETS_SHEET_NAME}'!A2:E").execute().get('values', [])
                    if not rows: is_default = True
                    if is_default:
                        for i, r in enumerate(rows):
                            if len(r) > 3 and r[3].upper() == 'TRUE':
                                service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{WALLETS_SHEET_NAME}'!D{i+2}", valueInputOption='USER_ENTERED', body={'values': [['FALSE']]}).execute()
                    new_row = [payload.get('name'), float(payload.get('balance', 0)), payload.get('wallet_type', 'bank_account'), str(is_default).upper(), str(uuid.uuid4())]
                    service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{WALLETS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [new_row]}).execute()
                    clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'reconcile_wallet':
                service = get_sheets_service()
                
                w_uuid = payload['wallet_uuid']; actual = float(payload['actual_balance'])
                rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{WALLETS_SHEET_NAME}'!A2:E").execute().get('values', [])
                idx = -1; current_bal = 0.0
                
                for i, r in enumerate(rows):
                    if len(r) >= 5 and r[4] == w_uuid:
                        idx = i
                        try: current_bal = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                        except: current_bal = 0.0
                        break
                
                if idx != -1:
                    diff = actual - current_bal
                    if abs(diff) > 0.01:
                        service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{WALLETS_SHEET_NAME}'!B{idx+2}", valueInputOption='USER_ENTERED', body={'values': [[actual]]}).execute()
                        row = [datetime.now(MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M:%S'), diff, "Корректировка", "Доход" if diff > 0 else "Расход", "Сверка баланса", init_data.user.first_name, str(uuid.uuid4()), w_uuid]
                        service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [row]}).execute()
                        clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'transfer_between_wallets':
                service = get_sheets_service()
                
                from_uuid = payload['from_wallet']; to_uuid = payload['to_wallet']; amt = abs(float(payload['amount']))
                d_str = payload.get('date'); fd = (datetime.strptime(d_str, '%Y-%m-%d').strftime('%d.%m.%Y') + datetime.now(MOSCOW_TIMEZONE).strftime(' %H:%M:%S')) if d_str else datetime.now(MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M:%S')
                
                update_wallet_balance(service, sid, from_uuid, -amt)
                update_wallet_balance(service, sid, to_uuid, amt)
                
                row_out = [fd, -amt, "Перевод", "Перевод", f"{payload.get('comment', 'Перевод')} (исход.)", init_data.user.first_name, str(uuid.uuid4()), from_uuid]
                row_in = [fd, amt, "Перевод", "Перевод", f"{payload.get('comment', 'Перевод')} (вход.)", init_data.user.first_name, str(uuid.uuid4()), to_uuid]
                service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [row_out, row_in]}).execute()
                clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            if action == 'manage_debt':
                print(f"[LOG] manage_debt payload: {payload}")
                service = get_sheets_service()
                op_type = payload.get('type')
                
                if op_type == 'add':
                    min_pay = float(payload.get('min_payment', 0))
                    row = [payload['name'], payload['debt_type'], float(payload['amount']), float(payload['rate']), str(uuid.uuid4()), min_pay]
                    service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [row]}).execute()
                    clear_user_cache(sid)
                
                elif op_type == 'repay':
                    print("[LOG] Repaying debt...")
                    rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'!A2:F").execute().get('values', [])
                    print(f"[LOG] Fetched {len(rows)} debt rows")
                    
                    target_id = str(payload.get('id')).strip()
                    payment = float(payload['amount'])
                    print(f"[LOG] Target ID: {target_id}, Payment: {payment}")
                    
                    idx = -1
                    for i, r in enumerate(rows):
                        if len(r) > 4:
                            print(f"[LOG] Checking row {i}: ID={r[4]}")
                            if r[4].strip() == target_id:
                                idx = i
                                break
                    
                    print(f"[LOG] Index found: {idx}")
                    if idx != -1:
                        raw_amount = rows[idx][2]
                        print(f"[LOG] Raw amount in sheet: {raw_amount}")
                        # --- SAFE PARSING ---
                        current_debt = float(str(raw_amount).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                        new_debt = max(0, current_debt - payment)
                        print(f"[LOG] Updating debt to: {new_debt}")
                        
                        service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'!C{idx+2}", valueInputOption='USER_ENTERED', body={'values': [[new_debt]]}).execute()
                        
                        is_expense = (rows[idx][1] == 'credit')
                        amt = -abs(payment) if is_expense else abs(payment)
                        
                        # --- FIX: ALWAYS FIND A WALLET ---
                        def_wallet = get_default_wallet_uuid(service, sid)
                        print(f"[LOG] Default wallet: {def_wallet}")
                        if def_wallet: update_wallet_balance(service, sid, def_wallet, amt)
                        
                        trans_row = [datetime.now(MOSCOW_TIMEZONE).strftime('%d.%m.%Y %H:%M:%S'), amt, "Кредиты" if is_expense else "Долги", "Расход" if is_expense else "Доход", f"Погашение: {rows[idx][0]}", init_data.user.first_name, str(uuid.uuid4()), def_wallet if def_wallet else ""]
                        service.spreadsheets().values().append(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'", valueInputOption='USER_ENTERED', body={'values': [trans_row]}).execute()
                        clear_user_cache(sid)
                    else:
                        print("[LOG] Debt ID not found in rows")
                        
                elif op_type == 'forgive':
                    rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'!A2:E").execute().get('values', [])
                    target_id = str(payload['id']).strip()
                    idx = next((i for i, r in enumerate(rows) if len(r)>4 and r[4].strip() == target_id), -1)
                    if idx != -1: service.spreadsheets().values().update(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'!C{idx+2}", valueInputOption='USER_ENTERED', body={'values': [[0]]}).execute()
                    clear_user_cache(sid)
                
                elif op_type == 'delete':
                    rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'!E:E").execute().get('values', [])
                    idx = next((i for i, r in enumerate(rows) if r and r[0].strip() == str(payload['id']).strip()), -1)
                    if idx != -1:
                        sheet_meta = service.spreadsheets().get(spreadsheetId=sid).execute()
                        sheet_id = next(s['properties']['sheetId'] for s in sheet_meta['sheets'] if s['properties']['title'] == DEBTS_SHEET_NAME)
                        service.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": [{"deleteDimension": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": idx, "endIndex": idx + 1}}}]}).execute()
                        clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}

            # --- GET DEBTS (UPDATED v3.7 with Emergency Logic) ---
            if action == 'get_debts':
                cache_key = get_cache_key(sid, 'get_debts', {})
                cached = get_from_cache(cache_key)
                if cached: return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}

                service = get_sheets_service()
                
                # Fetch Debts, Wallets, and Settings in one batch
                try:
                    res = service.spreadsheets().values().batchGet(spreadsheetId=sid, ranges=[f"'{DEBTS_SHEET_NAME}'!A2:F", f"'{WALLETS_SHEET_NAME}'!A2:B", f"'{BUDGET_SHEET_NAME}'!D2:E"]).execute()
                    d_rows = res['valueRanges'][0].get('values', [])
                    w_rows = res['valueRanges'][1].get('values', [])
                    s_rows = res['valueRanges'][2].get('values', [])
                except: await setup_sheet(sid); d_rows=[]; w_rows=[]; s_rows=[]

                # Parse Settings
                settings = {r[0]: r[1] for r in s_rows if len(r) >= 2}
                emergency_goal = float(settings.get('emergency_fund_goal', 0))

                # Parse Wallets (sum positive balances only)
                total_cash = 0.0
                for r in w_rows:
                    if len(r) >= 2:
                        try:
                            bal = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                            if bal > 0: total_cash += bal
                        except: pass

                debts_list = []; total_min_payment_needed = 0; total_owed_me = 0.0

                for r in d_rows:
                    if len(r) < 5: continue
                    try:
                        min_p = float(str(r[5]).replace(',', '.')) if len(r) > 5 and r[5] else 0.0
                        # --- SAFE PARSING ---
                        amt = float(str(r[2]).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                        
                        d_obj = {"id": r[4], "name": r[0], "type": r[1], "amount": amt, "rate": float(r[3]), "min_payment": min_p}
                        if d_obj['amount'] > 0:
                            debts_list.append(d_obj)
                            if r[1] == 'credit': total_min_payment_needed += min_p
                            else: total_owed_me += d_obj['amount']
                    except: continue

                # Initialize Strategist with Safety Net logic
                strategist = DebtStrategist(debts_list, extra_monthly_payment=0, current_savings=total_cash, emergency_goal=emergency_goal)
                s_avalanche = strategist.simulate_payoff('avalanche')
                s_snowball = strategist.simulate_payoff('snowball')

                credits = [d for d in debts_list if d['type'] == 'credit']
                credits.sort(key=lambda x: x['rate'], reverse=True)
                target_id = credits[0]['id'] if credits else None
                total_owe = sum(d['amount'] for d in credits)
                daily_pain = sum(d['amount'] * (d['rate'] / 100 / 365) for d in credits)

                result = {
                    "items": debts_list,
                    "total_owe": total_owe,
                    "total_owed_me": total_owed_me,
                    "total_min_payment": total_min_payment_needed,
                    "daily_pain": round(daily_pain, 2),
                    "target_debt_id": target_id,
                    "emergency_fund": {"current": total_cash, "goal": emergency_goal},
                    "analytics": {"avalanche": s_avalanche, "snowball": s_snowball, "freedom_date": s_avalanche['freedom_date']}
                }
                save_to_cache(cache_key, result)
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(result)}

            if action == 'get_history':
                cache_key = get_cache_key(sid, 'get_history', payload)
                cached = get_from_cache(cache_key)
                if cached: return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}
                service = get_sheets_service()
                offset = int(payload.get('offset', 0)); limit = 20
                rows = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{TRANSACTIONS_SHEET_NAME}'!A2:G").execute().get('values', [])
                hist = []
                rm = int(payload.get('month')) if payload.get('month') else None
                ry = int(payload.get('year')) if payload.get('year') else None
                for r in rows:
                    if len(r) < 2: continue
                    try: dt = datetime.strptime(r[0], '%d.%m.%Y %H:%M:%S')
                    except: continue
                    if rm and ry and (dt.month != rm or dt.year != ry): continue
                    try: amt = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                    except: continue
                    hist.append({"id": r[6] if len(r)>6 else None, "date": r[0], "amount": amt, "category": r[2] if len(r)>2 else "", "comment": r[4] if len(r)>4 else "", "author": r[5] if len(r)>5 else ""})
                hist.sort(key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y %H:%M:%S'), reverse=True)
                chunk = hist[offset : offset + limit]
                save_to_cache(cache_key, chunk)
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(chunk)}

            if action == 'get_summary':
                start = time.time()
                cache_key = get_cache_key(sid, 'get_summary', payload)
                cached = get_from_cache(cache_key)
                if cached:
                    print(f"[PERF] get_summary took {time.time() - start:.3f} sec (cache)")
                    return {'statusCode': 200, 'headers': cors, 'body': json.dumps(cached)}

                service = get_sheets_service()
                today_str = datetime.now(MOSCOW_TIMEZONE).date().isoformat()
                sub_run_key = f"{sid}:subscriptions_last_run"
                last_run = get_from_cache(sub_run_key)
                has_sub_updates = False
                if last_run != today_str:
                    has_sub_updates = await process_subscriptions(service, sid)
                    save_to_cache(sub_run_key, today_str, ttl=86400)
                if has_sub_updates:
                    print("[LOG] Subscriptions updated during summary calculation")
                
                ranges = [f"'{TRANSACTIONS_SHEET_NAME}'!A2:G", f"'{BUDGET_SHEET_NAME}'!A2:B"]
                try: resp = service.spreadsheets().values().batchGet(spreadsheetId=sid, ranges=ranges).execute()
                except: await setup_sheet(sid); resp = service.spreadsheets().values().batchGet(spreadsheetId=sid, ranges=ranges).execute()

                t_rows = resp.get('valueRanges', [])[0].get('values', []) if resp.get('valueRanges') else []
                b_rows = resp.get('valueRanges', [None, None])[1].get('values', []) if len(resp.get('valueRanges', [])) > 1 and resp.get('valueRanges')[1] else []
                
                limits = {} 
                for r in b_rows:
                    if len(r) >= 2:
                        try: limits[r[0]] = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', ''))
                        except: continue

                cat_q = f"SELECT category_id, category_name, category_type FROM `categories` WHERE telegram_id = {int(oid)};"
                cat_res = pool.retry_operation_sync(lambda s: s.transaction().execute(cat_q, commit_tx=True))
                cat_map = {} 
                if cat_res and cat_res[0].rows:
                    for r in cat_res[0].rows:
                        try: cn, ct, ci = r.category_name.decode('utf-8') if isinstance(r.category_name, bytes) else r.category_name, r.category_type.decode('utf-8') if isinstance(r.category_type, bytes) else r.category_type, r.category_id.decode('utf-8') if isinstance(r.category_id, bytes) else r.category_id
                        except: cn, ct, ci = r.category_name, r.category_type, r.category_id
                        cat_map[(cn, ct)] = ci

                rm = int(payload.get('month')) if payload.get('month') else None
                ry = int(payload.get('year')) if payload.get('year') else None

                bal, inc, exp = 0.0, 0.0, 0.0
                stats = {}; hist = []
                
                for r in t_rows:
                    if len(r) < 2: continue
                    try: dt = datetime.strptime(r[0], '%d.%m.%Y %H:%M:%S')
                    except: continue
                    if rm and ry and (dt.month != rm or dt.year != ry): continue
                    try: amt = float(str(r[1]).replace(',', '.').replace(' ', '').replace('\xa0', '').replace('₽', ''))
                    except: continue
                    bal += amt
                    cat_name = r[2] if len(r) > 2 and r[2] != "" else "Без категории"
                    is_transfer = (len(r)>3 and r[3] == "Перевод") or cat_name == "Перевод" or cat_name == "Корректировка"
                    
                    if not is_transfer:
                        t_type = "income" if amt > 0 else "expense"
                        if amt > 0: inc += amt
                        else: exp += amt
                        stats_key = (cat_name, t_type)
                        stats[stats_key] = stats.get(stats_key, 0.0) + amt
                    
                    hist.append({"id": r[6] if len(r)>6 else None, "date": r[0], "amount": amt, "category": cat_name, "comment": r[4] if len(r)>4 else "", "author": r[5] if len(r)>5 else ""})

                breakdown = []
                for (c_name, c_type), amount in stats.items():
                    limit_val = limits.get(c_name, 0) if c_type == 'expense' else 0
                    breakdown.append({'category': c_name, 'amount': amount, 'limit': limit_val, 'id': cat_map.get((c_name, c_type)), 'type': c_type})

                breakdown.sort(key=lambda x: abs(x['amount']), reverse=True)
                hist.sort(key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y %H:%M:%S'), reverse=True)
                
                analytics = {"daily_avg": 0, "monthly_forecast": 0}
                now = datetime.now(MOSCOW_TIMEZONE)
                if (rm is None and ry is None) or (rm == now.month and ry == now.year):
                    day_of_month = now.day; _, days_in_month = calendar.monthrange(now.year, now.month)
                    daily_avg = abs(exp) / day_of_month if day_of_month > 0 else 0
                    analytics = {"daily_avg": int(daily_avg), "monthly_forecast": int(daily_avg * days_in_month)}

                res_data = {"balance": bal, "income": inc, "expense": exp, "breakdown": breakdown, "history": hist[:20], "has_more": len(hist) > 20, "analytics": analytics}
                save_to_cache(cache_key, res_data, ttl=30)
                print(f"[PERF] get_summary took {time.time() - start:.3f} sec")
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(res_data)}

            # --- CALCULATE EXPENSE IMPACT (v3.5) ---
            if action == 'calculate_expense_impact':
                amount_to_check = float(payload.get('amount', 0))
                if amount_to_check <= 0:
                    return {'statusCode': 200, 'headers': cors, 'body': json.dumps({'days_delayed': 0, 'interest_cost': 0, 'percentage': 0, 'total_debt': 0})}

                service = get_sheets_service()
                
                # 1. Fetch current debts
                try:
                    resp = service.spreadsheets().values().get(spreadsheetId=sid, range=f"'{DEBTS_SHEET_NAME}'!A2:F").execute()
                    rows = resp.get('values', [])
                except: rows = []

                debts_list = []
                total_debt = 0.0
                for r in rows:
                    if len(r) < 5: continue
                    try:
                        min_p = float(str(r[5]).replace(',', '.')) if len(r) > 5 and r[5] else 0.0
                        d_obj = {"id": r[4], "name": r[0], "type": r[1], "amount": float(r[2]), "rate": float(r[3]), "min_payment": min_p}
                        if d_obj['amount'] > 0 and d_obj['type'] == 'credit': 
                            debts_list.append(d_obj)
                            total_debt += d_obj['amount']
                    except: continue

                # 2. Calculate Percentage
                percentage_of_total = (amount_to_check / total_debt * 100) if total_debt > 0 else 0

                # 3. Simulate Scenarios
                # При симуляции влияния мы не учитываем подушку, чтобы просто показать разницу во времени
                strategist = DebtStrategist(debts_list)
                # A: Baseline (Status Quo)
                base_scenario = strategist.simulate_payoff('avalanche', one_time_payment=0)
                # B: Invested (If we put this money into debt instead of spending)
                invest_scenario = strategist.simulate_payoff('avalanche', one_time_payment=amount_to_check)
                
                # 4. Difference
                months_diff = base_scenario['months_to_free'] - invest_scenario['months_to_free']
                interest_diff = base_scenario['total_interest'] - invest_scenario['total_interest']
                days_delayed = months_diff * 30
                
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps({
                    'days_delayed': days_delayed,
                    'interest_cost': round(interest_diff, 2),
                    'percentage': round(percentage_of_total, 1),
                    'total_debt': total_debt
                })}

            if action == 'get_family_members':
                query = f"SELECT telegram_id, first_name FROM `users` WHERE owner_id = {oid};"
                
                def callee(session):
                    return session.transaction().execute(query, commit_tx=True)
                
                res = pool.retry_operation_sync(callee)
                members = []
                
                if res and res[0].rows:
                    for row in res[0].rows:
                        mid = int(row.telegram_id)
                        fname = row.first_name.decode('utf-8') if isinstance(row.first_name, bytes) else row.first_name
                        members.append({"id": mid, "name": fname, "is_owner": mid == oid})
                
                result = {"members": members, "is_requester_owner": uid == oid}
                return {'statusCode': 200, 'headers': cors, 'body': json.dumps(result)}

            if action == 'kick_member':
                if uid != oid: return {'statusCode': 403, 'headers': cors, 'body': 'Not owner'}
                
                target_id = int(payload['id'])
                query = f"DELETE FROM `users` WHERE telegram_id = {target_id};"
                execute_query(query)
                clear_user_cache(sid)
                
                return {'statusCode': 200, 'headers': cors, 'body': '{}'}
            
            return {'statusCode': 200, 'headers': cors, 'body': '{}'}
            
    except Exception as e:
        print(f"[ERROR] CRITICAL: {e}")
        traceback.print_exc()
        return {'statusCode': 500, 'headers': cors, 'body': json.dumps({'error': str(e)})}