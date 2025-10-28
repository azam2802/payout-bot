import asyncio
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
import aiohttp
import json
from dateutil import parser as date_parser

from aiogram import Bot, Dispatcher, Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:3000")
PROCESSED_UUIDS_FILE = "processed_uuids.json"
USER_IDS_FILE = "user_ids.json"
AUTO_MODE_FILE = "auto_mode.json"
CHECK_INTERVAL = 5  # 1 minute
RECENT_MINUTES = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


class AutoModeStates(StatesGroup):
    """States for auto mode configuration"""
    waiting_for_range = State()


def load_processed_uuids():
    """Load processed UUIDs from file"""
    if Path(PROCESSED_UUIDS_FILE).exists():
        with open(PROCESSED_UUIDS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_processed_uuids(uuids):
    """Save processed UUIDs to file"""
    with open(PROCESSED_UUIDS_FILE, "w") as f:
        json.dump(list(uuids), f, indent=2)


def load_user_ids():
    """Load user IDs from file"""
    if Path(USER_IDS_FILE).exists():
        with open(USER_IDS_FILE, "r") as f:
            return set(json.load(f))
    return set()


def save_user_ids(user_ids):
    """Save user IDs to file"""
    with open(USER_IDS_FILE, "w") as f:
        json.dump(list(user_ids), f, indent=2)


def add_user_id(user_id):
    """Add user ID to the list"""
    user_ids = load_user_ids()
    user_ids.add(user_id)
    save_user_ids(user_ids)


def load_auto_mode():
    """Load auto mode status from file"""
    if Path(AUTO_MODE_FILE).exists():
        with open(AUTO_MODE_FILE, "r") as f:
            data = json.load(f)
            return data.get("enabled", False), data.get("min_amount"), data.get("max_amount")
    return False, None, None


def save_auto_mode(enabled, min_amount=None, max_amount=None):
    """Save auto mode status to file"""
    with open(AUTO_MODE_FILE, "w") as f:
        json.dump({
            "enabled": enabled,
            "min_amount": min_amount,
            "max_amount": max_amount
        }, f, indent=2)


async def fetch_payouts():
    """Fetch payouts from backend"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{BACKEND_URL}/payouts", timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("payouts", [])
    except Exception as e:
        print(f"Error fetching payouts: {e}")
    return []


async def fetch_pending_payouts():
    """Fetch pending payouts from backend"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{BACKEND_URL}/pending-payouts",
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("payouts", [])
    except Exception as e:
        print(f"Error fetching pending payouts: {e}")
    return []


def get_max_pending_amount(pending_payouts):
    """Get maximum amount from pending payouts"""
    if not pending_payouts:
        return 0
    try:
        amounts = [float(p.get("amount", 0)) for p in pending_payouts]
        return max(amounts) if amounts else 0
    except Exception:
        return 0


async def send_notification(user_id, message):
    """Send notification to user"""
    try:
        await bot.send_message(chat_id=user_id, text=message, parse_mode="HTML")
        return True
    except Exception as e:
        print(f"Error sending message to {user_id}: {e}")
        return False


async def send_manual_notification(payout):
    """Send manual notification with buttons for a payout"""
    user_ids = load_user_ids()

    if not user_ids:
        print("No users registered for notifications")
        return

    uuid = payout.get("uuid", "N/A")
    customer_name = payout.get("customer_name", "")
    customer_surname = payout.get("customer_surname", "")
    amount = payout.get("amount", "N/A")
    # Format amount with 1 decimal place and KGS currency
    try:
        amount_formatted = f"{float(amount):.1f} KGS"
    except (ValueError, TypeError):
        amount_formatted = f"{amount} KGS"

    # Format creation time: keep only date and time up to seconds
    creation_time_raw = payout.get("creation_time", "N/A")
    try:
        # Parse and format: "2025-10-18 18:56:16.050351+03" -> "2025-10-18 18:56:16"
        creation_time_str = creation_time_raw.split("+")[0]
        creation_time_formatted = creation_time_str.split(".")[0]
    except Exception:
        creation_time_formatted = creation_time_raw

    # Fetch pending payouts to check if we need to show list
    pending_payouts = await fetch_pending_payouts()
    pending_count = len(pending_payouts) if pending_payouts else 0

    keyboard = None
    message_text = (
        f"<b>🔔 Выплата больше текущих в обработке!</b>\n\n"
        f"<b>UUID:</b> <code>{uuid}</code>\n"
        f"<b>Клиент:</b> {customer_name} {customer_surname}\n"
        f"<b>Сумма:</b> {amount_formatted}\n"
        f"<b>Время создания:</b> {creation_time_formatted} (UTC +3)"
    )

    # If <= 4 payouts in progress, show simple "Accept" button
    if pending_count <= 4:
        # Store UUID in callback_data as base64 to avoid length issues
        import base64
        uuid_encoded = base64.b64encode(uuid.encode()).decode()
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_{uuid_encoded}")
            ]]
        )
    else:
        # If >= 5 payouts, show ALL payouts with cancel buttons
        pending_list = f"\n\n<b>⚠️ В обработке {pending_count} выплат (отмените некоторые перед принятием новой):</b>\n"
        for idx, payout_item in enumerate(pending_payouts, 1):
            p_uuid = payout_item.get("uuid", "N/A")
            p_name = payout_item.get("customer_name", "")
            p_surname = payout_item.get("customer_surname", "")
            p_amount = payout_item.get("amount", "N/A")
            p_time = payout_item.get("creation_time", "N/A")
            try:
                p_time = p_time.split("+")[0].split(".")[0]
            except:
                pass
            try:
                p_amount = f"{float(p_amount):.1f}"
            except:
                pass
            pending_list += f"{idx}. {p_name} {p_surname} - {p_amount} KGS ({p_time})\n"

        message_text += pending_list

        # Create numbered buttons for ALL payouts in queue
        # Store both new payout UUID and new payout count in callback_data
        import base64
        uuid_encoded = base64.b64encode(uuid.encode()).decode()
        buttons = []
        for idx in range(len(pending_payouts)):  # Show all payouts
            buttons.append(
                InlineKeyboardButton(
                    text=str(idx + 1),
                    callback_data=f"cancel_{idx}_{uuid_encoded}"
                )
            )

        keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])

    for user_id in user_ids:
        try:
            if keyboard:
                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode="HTML",
                    reply_markup=keyboard
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode="HTML"
                )
        except Exception as e:
            print(f"Error sending message to {user_id}: {e}")
        await asyncio.sleep(0.1)  # Rate limiting



async def handle_auto_mode(payout):
    """Handle automatic payout acceptance"""
    try:
        auto_enabled, min_amount, max_amount = load_auto_mode()
        
        pending_payouts = await fetch_pending_payouts()
        pending_count = len(pending_payouts) if pending_payouts else 0
        
        uuid = payout.get("uuid", "N/A")
        customer_name = payout.get("customer_name", "")
        customer_surname = payout.get("customer_surname", "")
        amount = payout.get("amount", "N/A")
        try:
            amount_formatted = f"{float(amount):.1f} KGS"
            amount_value = float(amount)
        except (ValueError, TypeError):
            amount_formatted = f"{amount} KGS"
            amount_value = 0
            
        print(f"[AUTO MODE] New payout: {uuid}, Amount: {amount}, Pending: {pending_count}")
        
        # Check if amount is in range
        if min_amount is not None and max_amount is not None:
            if not (min_amount <= amount_value <= max_amount):
                print(f"[AUTO MODE] Amount {amount_value} is outside range [{min_amount}, {max_amount}], sending manual notification")
                # Send manual notification with buttons (same as manual mode)
                await send_manual_notification(payout)
                return
        
        if pending_count < 5:
            # Simply accept the new payout
            print(f"[AUTO MODE] Accepting payout {uuid} (pending < 5)")
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{BACKEND_URL}/accept-payouts",
                    json={"ids": [uuid]},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        success_count = len(result.get("success", []))
                        print(f"[AUTO MODE] ✅ Accepted: {success_count} payouts")
        
                        # Notify users
                        user_ids = load_user_ids()
                        message = (
                            f"<b>🤖 Автоматически принято!</b>\n\n"
                            f"<b>UUID:</b> <code>{uuid}</code>\n"
                            f"<b>Клиент:</b> {customer_name} {customer_surname}\n"
                            f"<b>Сумма:</b> {amount_formatted} KGS\n"
                            f"<b>В обработке было:</b> {pending_count}/5"
                        )
                        for user_id in user_ids:
                            await send_notification(user_id, message)
                    else:
                        print(f"[AUTO MODE] ❌ Error accepting: {resp.status}")
        else:
            # Cancel the smallest payout and accept the new one
            print(f"[AUTO MODE] Pending >= 5, finding smallest to cancel")
            
            # Find the smallest payout
            smallest_payout = min(pending_payouts, key=lambda p: float(p.get("amount", 0)))
            smallest_uuid = smallest_payout.get("uuid")
            smallest_amount = smallest_payout.get("amount")
            smallest_name = smallest_payout.get("customer_name", "")
            smallest_surname = smallest_payout.get("customer_surname", "")
            
            print(f"[AUTO MODE] Cancelling smallest: {smallest_uuid}, Amount: {smallest_amount}")
            
            async with aiohttp.ClientSession() as session:
                # Cancel the smallest payout
                async with session.post(
                    f"{BACKEND_URL}/cancel-payouts",
                    json={"ids": [smallest_uuid]},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        cancel_result = await resp.json()
                        cancel_success = len(cancel_result.get("success", []))
                        print(f"[AUTO MODE] ✅ Cancelled: {cancel_success} payouts")
                        
                        # Accept the new payout
                        async with session.post(
                            f"{BACKEND_URL}/accept-payouts",
                            json={"ids": [uuid]},
                            timeout=aiohttp.ClientTimeout(total=30)
                        ) as accept_resp:
                            if accept_resp.status == 200:
                                accept_result = await accept_resp.json()
                                accept_success = len(accept_result.get("success", []))
                                print(f"[AUTO MODE] ✅ Accepted new: {accept_success} payouts")
                                
                                # Notify users
                                user_ids = load_user_ids()
                                message = (
                                    f"<b>🤖 Автоматически принято!</b>\n\n"
                                    f"<b>❌ Отменено:</b>\n"
                                    f"Клиент: {smallest_name} {smallest_surname}\n"
                                    f"Сумма: {smallest_amount} KGS\n\n"
                                    f"<b>✅ Принято:</b>\n"
                                    f"Клиент: {customer_name} {customer_surname}\n"
                                    f"Сумма: {amount} KGS\n\n"
                                    f"<b>В обработке было:</b> {pending_count}/5"
                                )
                                for user_id in user_ids:
                                    await send_notification(user_id, message)
                            else:
                                print(f"[AUTO MODE] ❌ Error accepting new: {accept_resp.status}")
                    else:
                        print(f"[AUTO MODE] ❌ Error cancelling: {resp.status}")
                        
    except Exception as e:
        print(f"[AUTO MODE] Error: {e}")
        import traceback
        traceback.print_exc()



async def notify_users(payout):
    """Send notification to all users about new high-value payout"""
    auto_enabled, min_amount, max_amount = load_auto_mode()
    
    # If auto mode is enabled, handle automatically
    if auto_enabled:
        await handle_auto_mode(payout)
        return
    
    # Manual mode - send notification with buttons
    await send_manual_notification(payout)


async def check_payouts():
    """Main function to check and notify about payouts"""
    processed_uuids = load_processed_uuids()

    try:
        # Fetch data from backend
        all_payouts = await fetch_payouts()
        pending_payouts = await fetch_pending_payouts()

        if (
            pending_payouts is None
            or len(pending_payouts) == 0
            or len(all_payouts) == 0
            or all_payouts is None
        ):
            print(f"[{datetime.now()}] No payouts data.")
            return

        pending_amounts = []
        for pending in pending_payouts:
            try:
                amount_val = float(pending.get("amount", 0))
                if math.isfinite(amount_val):
                    pending_amounts.append(amount_val)
            except (ValueError, TypeError):
                continue
        if not pending_amounts:
            print(f"[{datetime.now()}] Pending payouts contain no comparable amounts.")
            return

        # Filter recent payouts not yet processed
        recent_new_payouts = []
        for payout in all_payouts:
            uuid = payout.get("uuid")
            if uuid and uuid not in processed_uuids:
                try:
                    amount = float(payout.get("amount", 0))
                    if any(amount > pending_amount for pending_amount in pending_amounts):
                        # Parse creation time using dateutil (flexible parsing)
                        creation_time_str = payout.get("creation_time", "")

                        # Parse datetime with timezone info
                        creation_time = date_parser.parse(creation_time_str)
                        # Convert to UTC timezone-aware datetime
                        if creation_time.tzinfo is None:
                            creation_time = creation_time.replace(tzinfo=timezone.utc)
                        else:
                            creation_time = creation_time.astimezone(timezone.utc)

                        current_time_utc = datetime.now(timezone.utc)

                        if creation_time >= current_time_utc - timedelta(
                            minutes=RECENT_MINUTES
                        ):
                            recent_new_payouts.append(payout)
                except (ValueError, TypeError) as e:
                    print(f"Error parsing payout time: {creation_time_str} - {e}")
                    pass

        # Send notifications and update processed UUIDs
        if recent_new_payouts:
            print(
                f"Found {len(recent_new_payouts)} new high-value payouts. Total: {len(all_payouts)}, Pending: {len(pending_payouts)}"
            )
            for payout in recent_new_payouts:
                await notify_users(payout)
                processed_uuids.add(payout["uuid"])

            save_processed_uuids(processed_uuids)
        else:
            print(
                f"[{datetime.now()}] No new high-value payouts. Total: {len(all_payouts)}, Pending: {len(pending_payouts)}"
            )

    except Exception as e:
        print(f"Error in check_payouts: {e}")


async def periodic_check():
    """Run periodic check every minute"""
    while True:
        try:
            await check_payouts()
        except Exception as e:
            print(f"Error in periodic check: {e}")

        await asyncio.sleep(CHECK_INTERVAL)


@router.message(Command("start"))
async def handle_start(message: types.Message):
    """Handle /start command from user"""
    user_id = message.from_user.id
    add_user_id(user_id)
    await message.reply(
        f"✅ Вы подписаны на уведомления о выплатах!\n" f"Ваш ID: {user_id}"
    )


@router.message(Command("stop"))
async def handle_stop(message: types.Message):
    """Handle /stop command from user"""
    user_id = message.from_user.id
    user_ids = load_user_ids()
    user_ids.discard(user_id)
    save_user_ids(user_ids)
    await message.reply("❌ Вы отписаны от уведомлений")


@router.message(Command("status"))
async def handle_status(message: types.Message):
    """Handle /status command"""
    processed_uuids = load_processed_uuids()
    pending_payouts = await fetch_pending_payouts()
    auto_enabled, min_amount, max_amount = load_auto_mode()
    
    mode_text = "🤖 Автоматический" if auto_enabled else "👤 Ручной"
    if auto_enabled and min_amount is not None and max_amount is not None:
        mode_text += f" ({min_amount}-{max_amount} KGS)"

    message_text = (
        f"<b>📊 Статус бота</b>\n\n"
        f"<b>Режим:</b> {mode_text}\n"
        f"<b>Обработано выплат:</b> {len(processed_uuids)}\n"
        f"<b>В обработке:</b> {len(pending_payouts)}\n"
        f"<b>Макс сумма в обработке:</b> {get_max_pending_amount(pending_payouts)}"
    )
    await message.reply(message_text, parse_mode="HTML")


@router.message(Command("update"))
async def handle_update(message: types.Message):
    """Handle /update command - manually trigger data check"""
    await message.reply("⏳ Проверяю выплаты...")
    await check_payouts()
    await message.reply("✅ Проверка завершена")


@router.message(Command("mode"))
async def handle_mode(message: types.Message, state: FSMContext):
    """Handle /mode command - toggle auto mode"""
    auto_enabled, min_amount, max_amount = load_auto_mode()
    
    if auto_enabled:
        # Disable auto mode
        save_auto_mode(False)
        await message.reply(
            "👤 <b>Ручной режим ВКЛЮЧЕН</b>\n\n"
            "Бот будет отправлять уведомления с кнопками для ручного управления.",
            parse_mode="HTML"
        )
    else:
        # Ask for range before enabling
        await message.reply(
            "🤖 <b>Настройка автоматического режима</b>\n\n"
            "Укажите диапазон сумм для автоматического принятия в формате:\n"
            "<code>минимум-максимум</code>\n\n"
            "Например: <code>5000-10000</code>\n\n"
            "Или отправьте <code>0</code> для принятия всех выплат без ограничений.",
            parse_mode="HTML"
        )
        await state.set_state(AutoModeStates.waiting_for_range)


@router.message(AutoModeStates.waiting_for_range)
async def handle_range_input(message: types.Message, state: FSMContext):
    """Handle range input for auto mode"""
    user_input = message.text.strip()
    
    try:
        if user_input == "0":
            # No limits
            save_auto_mode(True, None, None)
            await message.reply(
                "🤖 <b>Автоматический режим ВКЛЮЧЕН</b>\n\n"
                "Диапазон: <b>БЕЗ ОГРАНИЧЕНИЙ</b>\n\n"
                "Бот будет автоматически принимать все новые выплаты:\n"
                "• Если в обработке &lt;5 выплат → принять новую\n"
                "• Если в обработке ≥5 выплат → отменить самую маленькую и принять новую",
                parse_mode="HTML"
            )
        else:
            # Parse range
            parts = user_input.split("-")
            if len(parts) != 2:
                raise ValueError("Invalid format")
            
            min_amount = float(parts[0].strip())
            max_amount = float(parts[1].strip())
            
            if min_amount < 0 or max_amount < 0 or min_amount > max_amount:
                raise ValueError("Invalid range")
            
            save_auto_mode(True, min_amount, max_amount)
            await message.reply(
                f"🤖 <b>Автоматический режим ВКЛЮЧЕН</b>\n\n"
                f"Диапазон: <b>{min_amount:.0f} - {max_amount:.0f} KGS</b>\n\n"
                f"Бот будет автоматически принимать новые выплаты в указанном диапазоне:\n"
                f"• Если в обработке &lt;5 выплат → принять новую\n"
                f"• Если в обработке ≥5 выплат → отменить самую маленькую и принять новую",
                parse_mode="HTML"
            )
        
        await state.clear()
        
    except Exception as e:
        await message.reply(
            "❌ <b>Ошибка!</b>\n\n"
            "Неверный формат. Укажите диапазон в формате:\n"
            "<code>минимум-максимум</code>\n\n"
            "Например: <code>5000-10000</code>\n"
            "Или <code>0</code> для всех выплат без ограничений.",
            parse_mode="HTML"
        )



@router.callback_query(lambda c: c.data.startswith("accept_"))
async def handle_accept_callback(callback_query: types.CallbackQuery):
    """Handle accept button click"""
    # Disable all buttons to prevent double-click
    await callback_query.message.edit_reply_markup(reply_markup=None)
    await callback_query.answer("⏳ Принимаю выплату...", show_alert=False)
    
    try:
        # Extract UUID from callback_data (it's base64 encoded)
        import base64
        callback_data = callback_query.data
        uuid_encoded = callback_data.replace("accept_", "")
        
        try:
            payout_uuid = base64.b64decode(uuid_encoded).decode()
        except Exception as e:
            print(f"Error decoding UUID: {e}")
            payout_uuid = None
        
        print(f"Extracted UUID: {payout_uuid}")
        
        if not payout_uuid or payout_uuid == "N/A":
            await callback_query.message.edit_text(
                callback_query.message.text + "\n\n❌ Ошибка: не удалось извлечь UUID",
                parse_mode="HTML"
            )
            return
        
        print(f"Sending accept request for UUID: {payout_uuid}")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{BACKEND_URL}/accept-payouts",
                json={"ids": [payout_uuid]},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                print(f"Response status: {resp.status}")
                response_data = await resp.json()
                print(f"Response data: {response_data}")
                
                if resp.status == 200:
                    result = response_data
                    success_count = len(result.get("success", []))
                    error_count = len(result.get("error", {}))
                    
                    response_text = f"✅ Принято: {success_count} выплат"
                    if error_count > 0:
                        error_details = result.get("error", {})
                        response_text += f"\n❌ Ошибки: {error_count}\n{str(error_details)}"
                    
                    await callback_query.message.edit_text(
                        callback_query.message.text + f"\n\n{response_text}",
                        parse_mode="HTML"
                    )
                else:
                    error_msg = await resp.text()
                    await callback_query.message.edit_text(
                        callback_query.message.text + f"\n\n❌ Ошибка ({resp.status}): {error_msg}",
                        parse_mode="HTML"
                    )
    except Exception as e:
        print(f"Error accepting payout: {e}")
        import traceback
        traceback.print_exc()
        await callback_query.message.edit_text(
            callback_query.message.text + f"\n\n❌ Ошибка: {str(e)}",
            parse_mode="HTML"
        )


@router.callback_query(lambda c: c.data.startswith("cancel_"))
async def handle_cancel_callback(callback_query: types.CallbackQuery):
    """Handle cancel button click"""
    # Parse callback data: cancel_{cancel_index}_{new_uuid_encoded}
    import base64
    parts = callback_query.data.split("_", 2)
    try:
        cancel_index = int(parts[1])
        new_uuid_encoded = parts[2] if len(parts) > 2 else None
    except (ValueError, IndexError):
        await callback_query.answer("❌ Некорректные данные кнопки", show_alert=True)
        return
    
    # Decode new payout UUID
    new_payout_uuid = None
    if new_uuid_encoded:
        try:
            new_payout_uuid = base64.b64decode(new_uuid_encoded).decode()
        except Exception as e:
            print(f"Error decoding new UUID: {e}")
    
    print(f"Cancel index: {cancel_index}, New payout UUID: {new_payout_uuid}")
    
    # Disable all buttons to prevent double-click
    await callback_query.message.edit_reply_markup(reply_markup=None)
    await callback_query.answer("⏳ Отменяю выплату...", show_alert=False)
    
    try:
        # Get current pending payouts to find which ones to cancel
        pending_payouts = await fetch_pending_payouts()
        print(f"Pending payouts count: {len(pending_payouts)}, cancel_index: {cancel_index}")
        
        if cancel_index >= len(pending_payouts):
            await callback_query.message.edit_text(
                callback_query.message.text + "\n\n❌ Ошибка: индекс выплаты некорректен",
                parse_mode="HTML"
            )
            return
        
        cancel_uuid = pending_payouts[cancel_index].get("uuid")
        print(f"Cancel UUID: {cancel_uuid}")
        
        async with aiohttp.ClientSession() as session:
            # Cancel existing payout
            print(f"Cancelling payout: {cancel_uuid}")
            async with session.post(
                f"{BACKEND_URL}/cancel-payouts",
                json={"ids": [cancel_uuid]},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                print(f"Cancel response status: {resp.status}")
                cancel_result = await resp.json()
                print(f"Cancel result: {cancel_result}")
                
                if resp.status == 200:
                    cancel_success = len(cancel_result.get("success", []))
                    cancel_errors = cancel_result.get("error", {})
                    
                    # Fetch updated pending payouts
                    updated_pending = await fetch_pending_payouts()
                    updated_pending_count = len(updated_pending) if updated_pending else 0
                    print(f"After cancel: pending count = {updated_pending_count}")
                    
                    # If still >= 5 pending, show list again and ask to continue cancelling
                    if updated_pending_count >= 5:
                        print(f"Still {updated_pending_count} pending, asking to cancel more")
                        
                        # Rebuild the list of pending payouts
                        response_text = (
                            f"✅ Отменено: {cancel_success} выплат\n\n"
                            f"<b>⚠️ В обработке {updated_pending_count} выплат (отмените ещё):</b>\n"
                        )
                        for idx, payout_item in enumerate(updated_pending, 1):
                            p_name = payout_item.get("customer_name", "")
                            p_surname = payout_item.get("customer_surname", "")
                            p_amount = payout_item.get("amount", "N/A")
                            p_time = payout_item.get("creation_time", "N/A")
                            try:
                                p_time = p_time.split("+")[0].split(".")[0]
                            except:
                                pass
                            try:
                                p_amount = f"{float(p_amount):.1f}"
                            except:
                                pass
                            response_text += f"{idx}. {p_name} {p_surname} - {p_amount} KGS ({p_time})\n"
                        
                        # Create new buttons for updated list
                        import base64
                        uuid_encoded = base64.b64encode(new_payout_uuid.encode()).decode()
                        buttons = []
                        for idx in range(len(updated_pending)):
                            buttons.append(
                                InlineKeyboardButton(
                                    text=str(idx + 1),
                                    callback_data=f"cancel_{idx}_{uuid_encoded}"
                                )
                            )
                        new_keyboard = InlineKeyboardMarkup(inline_keyboard=[buttons])
                        
                        await callback_query.message.edit_text(
                            response_text,
                            parse_mode="HTML",
                            reply_markup=new_keyboard
                        )
                    else:
                        # Less than 5 pending, proceed to accept new payout
                        print(f"Now {updated_pending_count} pending (< 5), accepting new payout")
                        accept_text = ""
                        if new_payout_uuid and new_payout_uuid != "N/A":
                            print(f"Accepting new payout: {new_payout_uuid}")
                            async with session.post(
                                f"{BACKEND_URL}/accept-payouts",
                                json={"ids": [new_payout_uuid]},
                                timeout=aiohttp.ClientTimeout(total=30)
                            ) as accept_resp:
                                print(f"Accept response status: {accept_resp.status}")
                                accept_result = await accept_resp.json()
                                print(f"Accept result: {accept_result}")
                                
                                if accept_resp.status == 200:
                                    accept_success = len(accept_result.get("success", []))
                                    accept_errors = accept_result.get("error", {})
                                    if accept_success > 0:
                                        accept_text = f"\n✅ Принято: {accept_success} новых выплат"
                                    if accept_errors:
                                        accept_text += f"\n❌ Ошибки принятия: {str(accept_errors)}"
                        
                        response_text = f"✅ Отменено: {cancel_success} выплат{accept_text}"
                        if cancel_errors:
                            response_text += f"\n❌ Ошибки отмены: {str(cancel_errors)}"
                        
                        await callback_query.message.edit_text(
                            callback_query.message.text + f"\n\n{response_text}",
                            parse_mode="HTML"
                        )
                else:
                    error_msg = await resp.text()
                    await callback_query.message.edit_text(
                        callback_query.message.text + f"\n\n❌ Ошибка отмены ({resp.status}): {error_msg}",
                        parse_mode="HTML"
                    )
    except Exception as e:
        print(f"Error cancelling payout: {e}")
        import traceback
        traceback.print_exc()
        await callback_query.message.edit_text(
            callback_query.message.text + f"\n\n❌ Ошибка: {str(e)}",
            parse_mode="HTML"
        )


async def main():
    """Main entry point"""
    # Create tasks for bot polling and periodic check
    check_task = asyncio.create_task(periodic_check())
    polling_task = asyncio.create_task(dp.start_polling(bot))

    try:
        await asyncio.gather(check_task, polling_task)
    except asyncio.CancelledError:
        print("Bot stopped")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    print("Bot started...")
    asyncio.run(main())
