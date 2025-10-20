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

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:3000")
PROCESSED_UUIDS_FILE = "processed_uuids.json"
USER_IDS_FILE = "user_ids.json"
CHECK_INTERVAL = 30  # 1 minute
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

    message = (
        f"<b>🔔 Выплата больше текущих в обработке!</b>\n\n"
        f"<b>UUID:</b> <code>{uuid}</code>\n"
        f"<b>Клиент:</b> {customer_name} {customer_surname}\n"
        f"<b>Сумма:</b> {amount_formatted}\n"
        f"<b>Время создания:</b> {creation_time_formatted} (UTC +3)"
    )

    for user_id in user_ids:
        await send_notification(user_id, message)
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
