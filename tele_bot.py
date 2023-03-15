"""Simple telegram bot for ALLO_XIAOMI PARSER SCRIPT"""

import redis.asyncio as redis
import json
from os import getenv

from asyncio import sleep

from aiogram import Bot, Dispatcher, executor, types
from aiogram.dispatcher.filters import Text

from async2_main import scrap_it, analyze_it, prepare_dir

import logging


# Make data dir if it was not created before
prepare_dir()

logging.basicConfig(filename='logs/tele_bot.log', encoding='utf-8', level=logging.DEBUG)


bot = Bot(token=getenv("TOKEN"))
dp = Dispatcher(bot)


connection_redis = redis.Redis()


# TODO: add redis channel listener for signal from cron-parser
# TODO: make redis in-localhost only (check!)
# https://stackoverflow.com/questions/40114913/allow-redis-connections-from-only-localhost

# TODO: try this https://stackoverflow.com/a/61757914 for redis var en/decoding

async def user_register(user):
    print(f"New USER was registered: {user.full_name} (ID:{user.id})")

    users = await connection_redis.get("allo_xiaomi_users")
    if users:
        users = json.loads(users)
        new_users = set(users)
        new_users.add(user.id)
    else:
        new_users = [user.id]

    await connection_redis.mset({"allo_xiaomi_users": json.dumps(list(new_users))})

    print(f"All users pack: {new_users}")


# TODO: unregister user and all user links for analyze? or just hold...
@dp.message_handler(commands="stop")
async def stop(message: types.Message):
    pass


@dp.message_handler(commands="start")
async def start(message: types.Message):

    # TODO: check if user is in DB/cache

    # message.from_id, message.from_user
    await user_register(user=message.from_user)

    start_buttons = ["MANUAL SCRAP XIOAMI", "SCRAP AND ANALYZE XIOAMI"]
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(*start_buttons)

    await message.answer("You can parse by button", reply_markup=keyboard)


@dp.message_handler(Text(equals="MANUAL SCRAP XIOAMI"))
async def manual_check(message: types.Message):
    await message.answer("Wait for result...")

    scrap_result = await scrap_it()
    await message.answer(scrap_result)

    filename = types.InputFile(scrap_result)
    await message.answer_document(filename)
    print(f"File {filename} was sent.")


@dp.message_handler(Text(equals="SCRAP AND ANALYZE XIOAMI"))
async def scrap_n_analyze(message: types.Message):
    await message.answer("Wait for result...")

    analyze_result, analyze_msgs = await analyze_it()

    if not analyze_result:
        await message.answer(f"Hm... {analyze_msgs}")
        return

    for msg in analyze_msgs:
        await message.answer(f"NEW!!!\n{msg}")
        await sleep(0.5)


async def send_signal(text, user=None):
    # if msg to concrete user id
    if user:
        print(f'Sending signal to {user}')
        await bot.send_message(user, text)
        return

    # if not - send to all users
    users = await connection_redis.get("allo_xiaomi_users")
    if users:
        users = json.loads(users)
        for user in users:
            print(f'Sending signal to {user}')
            await bot.send_message(user, text)


# TODO Pub/Sub listening
async def periodic_primitive_redis_listener(seconds=60):
    while True:
        await sleep(seconds)
        output = await connection_redis.get("allo_xiaomi_signal")
        if output:
            signal = json.loads(output)
            print(signal)
            await send_signal(signal["text"], user=signal.get("user", None))
            await connection_redis.mset({"allo_xiaomi_signal": ""})
        else:
            print("no signals")


async def periodic_primitive_data_analyzer(seconds=3600):
    while True:
        await sleep(seconds)
        analyze_result, analyze_msgs = await analyze_it()
        if analyze_result:
            # TODO: maybe check users before analyzing? because who needs this if now users
            users = await connection_redis.get("allo_xiaomi_users")
            if users:
                users = json.loads(users)
                for user in users:
                    for msg in analyze_msgs:
                        _msg = {
                            "user": user,
                            "text": f"NEW!\n{msg}",
                        }
                        # TODO: add msgs to some queque or background task!!! Not this!
                        await connection_redis.mset({"allo_xiaomi_signal": json.dumps(_msg)})
                        await sleep(6)


async def add_func(dp):
    dp._loop_create_task(periodic_primitive_redis_listener(seconds=3))
    dp._loop_create_task(periodic_primitive_data_analyzer(seconds=60*60))


if __name__ == "__main__":
    executor.start_polling(dp, on_startup=add_func)

# TODO: script arguments
# TODO: --no-verbose , logging
