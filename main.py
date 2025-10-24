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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:3000")
PROCESSED_UUIDS_FILE = "processed_uuids.json"
USER_IDS_FILE = "user_ids.json"
CHECK_INTERVAL = 5  # 5 seconds
RECENT_MINUTES = 5

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


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


def format_amount(amount):
    """Format amount with thousands separator and 1 decimal place"""
    try:
        value = float(amount)
        # Format with 1 decimal place
        formatted = f"{value:.1f}"
        # Split into parts
        parts = formatted.split('.')
        integer_part = parts[0]
        decimal_part = parts[1] if len(parts) > 1 else '0'
        
        # Add thousands separator
        integer_with_sep = '{:,}'.format(int(integer_part)).replace(',', ' ')
        
        # Only show decimal if it's not 0
        if decimal_part == '0':
            return integer_with_sep
        return f"{integer_with_sep} {decimal_part}"
    except (ValueError, TypeError):
        return str(amount)


async def send_notification(user_id, message):
    """Send notification to user"""
    try:
        await bot.send_message(chat_id=user_id, text=message, parse_mode="HTML")
        return True
    except Exception as e:
        print(f"Error sending message to {user_id}: {e}")
        return False


async def notify_users(payout):
    """Send notification to all users about new high-value payout"""
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
        amount_formatted = f"{format_amount(amount)} KGS"
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
        f"<b>🔔 Новая выплата!</b>\n\n"
        f"<b>Сумма:</b> {amount_formatted}\n"
        f"<b>UUID:</b> <code>{uuid}</code>\n"
        f"<b>Клиент:</b> {customer_name} {customer_surname}\n"
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
                p_amount = f"{format_amount(p_amount)}"
            except:
                pass
            pending_list += f"{idx}. {p_name} {p_surname} - {p_amount} KGS\n"

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

    message_text = (
        f"<b>📊 Статус бота</b>\n\n"
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
                                p_amount = f"{format_amount(p_amount)}"
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
