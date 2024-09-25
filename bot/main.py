import asyncio

from schedule_manager import schedule_notifier
from schedule_changing_manager import check_for_updates


async def main():
    await asyncio.gather(
        schedule_notifier(),
        check_for_updates()
    )

if __name__ == '__main__':
    asyncio.run(main())