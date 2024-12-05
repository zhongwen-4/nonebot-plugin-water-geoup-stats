import aiosqlite

from nonebot import on_command, on_message, require, get_driver, logger
from nonebot.adapters.onebot.v11 import GroupMessageEvent
from nonebot.plugin import PluginMetadata


__plugin_meta__ = PluginMetadata(
    name="发言统计",
    description="统计群聊发言情况",
    usage="""
    统计群聊发言情况
    """,
    type="application",
    homepage="https://github.com/zhongwen-4/nonebot-plugin-water-geoup-stats",
    supported_adapters={"~onebot.v11"},
)

require("nonebot_plugin_localstore")
require("nonebot_plugin_apscheduler")

from nonebot_plugin_localstore import get_plugin_data_file

path = get_plugin_data_file("data.db")
get_msg = on_message(block= False)
get_day_stats = on_command("发言统计")
get_month_stats = on_command("月发言统计")
driver = get_driver()


@driver.on_startup
async def _():

    logger.info("正在初始化数据库...")
    async with aiosqlite.connect(path) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS day_msg (user_id INTEGER, msg_count INTEGER, group_id INTEGER)"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS month_msg (user_id INTEGER, msg_count INTEGER, group_id INTEGER)"
        )
        await db.commit()
    logger.info("数据库初始化完成！即将开启定时任务...")


    from nonebot_plugin_apscheduler import scheduler
    @scheduler.scheduled_job("cron", hour=0, minute=0)
    async def _():
        async with aiosqlite.connect(path) as db:
            await db.execute(
                "UPDATE day_msg SET msg_count = 0"
            )
            await db.commit()
            logger.info("已清空每日发言数据")
    
    @scheduler.scheduled_job("cron", month='1-12', day='1', hour='0', minute='0')
    async def _():
        async with aiosqlite.connect(path) as db:
            await db.execute(
                "UPDATE month_msg SET msg_count = 0"
            )
            await db.commit()
            logger.info("已清空每月发言数据")
    logger.info("定时任务已开启！")


@get_msg.handle()
async def _(event: GroupMessageEvent):
    async with aiosqlite.connect(path) as db:
        is_user = await db.execute(
            "SELECT user_id, group_id FROM day_msg WHERE user_id = ?", (event.user_id,)
        )
        is_user = await is_user.fetchall()
        if is_user:
            is_group = [i[1] for i in is_user]

        if not is_user or event.group_id not in is_group:
            await db.execute(
                "INSERT INTO day_msg (user_id, msg_count, group_id) VALUES (?, 1, ?)",
                (event.user_id, event.group_id),
            )
            await db.execute(
                "INSERT INTO month_msg (user_id, msg_count, group_id) VALUES (?, 1, ?)",
                (event.user_id, event.group_id),
            )
        else:
            if event.group_id in is_group:
                await db.execute(
                    "UPDATE day_msg SET msg_count = msg_count + 1 WHERE user_id = ? AND group_id = ?",
                    (event.user_id, event.group_id,),
                )
                await db.execute(
                    "UPDATE month_msg SET msg_count = msg_count + 1 WHERE user_id = ? AND group_id = ?",
                    (event.user_id, event.group_id,),
                )
        await db.commit()


@get_day_stats.handle()
async def _(event: GroupMessageEvent):
    async with aiosqlite.connect(path) as db:
        msg_count = await db.execute(
            "SELECT msg_count, user_id FROM day_msg WHERE group_id = ? ORDER BY msg_count DESC",
            (event.group_id,)
        )
        msg_count = await msg_count.fetchall()

        if msg_count:
            msg_zhanbi = [i[0] for i in msg_count]
            zhanbi = sum(msg_zhanbi)
            percentages = [(number / zhanbi) for number in msg_zhanbi]
            user_data = [[i[0], i[1]] for i in msg_count]
        
        msg = []
        msg.append(f"本群今日发言统计:")
        msg.append("-------------------")

        for i, percentage in enumerate(percentages):
            if len(msg) >= 10:
                break

            if user_data[i][0] == 0:
                continue

            if msg_count != None:
                msg.append(f"{user_data[i][1]} 共发言 {user_data[i][0]} 条，占比 {percentage:.2%}")
        
        await get_day_stats.finish("\n".join(msg))


@get_month_stats.handle()
async def _(event: GroupMessageEvent):
    async with aiosqlite.connect(path) as db:
        msg_count = await db.execute(
            "SELECT msg_count, user_id FROM month_msg WHERE group_id = ? ORDER BY msg_count DESC",
            (event.group_id,)
        )
        msg_count = await msg_count.fetchall()

        if msg_count:
            msg_zhanbi = [i[0] for i in msg_count]
            zhanbi = sum(msg_zhanbi)
            percentages = [(number / zhanbi) for number in msg_zhanbi]
            user_data = [[i[0], i[1]] for i in msg_count]
        
        msg = []
        msg.append(f"本群本月发言统计:")
        msg.append("-------------------")
        for i, percentage in enumerate(percentages):

            if user_data[i][0] == 0:
                continue

            if msg_count != None:
                msg.append(f"{user_data[i][1]} 共发言 {user_data[i][0]} 条，占比 {percentage:.2%}")

            if len(msg) >= 10:
                break

        await get_month_stats.finish("\n".join(msg))