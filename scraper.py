import argparse
import asyncio

import aiohttp
from bs4 import BeautifulSoup

from models import GroupManager, Post


async def download_vk_group(group_name, offset, number, coroutines):
    async with aiohttp.ClientSession() as session:
        url = f"https://vk.com/{group_name}"
        async with session.get(url) as r:
            # calling group's url to init GroupManager object
            assert r.status == 200, f'{r.status}: {r.reason}. url: {r.url}'
            html = await r.text()
            soup = BeautifulSoup(html, features="html.parser")
            vk_group = GroupManager(soup, number)

        start = vk_group.latest_post - offset
        if start - number > 0:
            end = start - number
        else:
            end = 1

        # download continues until number of posts, added to vk_group, reaches requested 'number'
        posts_left = number - len(vk_group.posts)
        while posts_left > 0:
            await run_tasks(start, end, vk_group, session, coroutines)
            if start == 1:
                break
            start = end
            posts_left = number - len(vk_group.posts)
            # urls which 504'd are added to the next iteration
            end = start - len(vk_group.error_504) - 20
            if end < 1:
                end = 1
            print(f'offset: {vk_group.latest_post - start}, saved: {len(vk_group.posts)}')

        vk_group.save_to_csv()
        last_post = vk_group.posts[-1].number
        print(f"Posts saved: {len(vk_group.posts)}.")
        print(f"Last saved post's id: {last_post}.")
        if start == 1:
            print('\nGroup was scraped down to the 1st post.')


async def run_tasks(start, end, vk_group, session, coroutines):
    tasks = []
    # urls which 504'd are tried again
    tasks.extend(vk_group.error_504)
    vk_group.error_504 = []
    for post_id in range(start, end, -1):
        url = f'https://vk.com/wall-{vk_group.id}_{post_id}'
        task = asyncio.create_task(download_post(session, url, vk_group))
        tasks.append(task)
    await gather_with_concurrency(*tasks, n=coroutines)


async def download_post(session, url, vk_group):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/91.0.4472.77 Safari/537.36'

    # requests are made via Splash to have JS rendered
    async with session.post(f'http://127.0.0.1:8050/render.html', json={
        'url': url,
        'headers': {'User-Agent': user_agent},
        'wait': 5,
        'images': 0,
    }) as r:

        # this block filters out invalid pages
        if r.status == 504:
            vk_group.error_504.append(url)
            return
        assert r.status == 200, f'{r.status}: {r.reason}. url: {r.url}'
        html = await r.text()
        soup = BeautifulSoup(html, features="html.parser")

        post_url = soup.find('link', attrs={'rel': 'alternate'}).attrs['href']
        if post_url.find('?reply=') != -1:  # check if it is post or a comment
            return

        if soup.find('div', class_='message_page_body'):  # css class for error messages
            return

        # valid posts are saved
        post = Post(soup, vk_group)
        await post.save_images(session)
        vk_group.posts.append(post)


async def gather_with_concurrency(*tasks, n):
    # semaphore limits number of simultaneous coroutines
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


cli_parser = argparse.ArgumentParser(description='Save a vk.com group as .csv and images')
cli_parser.add_argument('name',
                        metavar='name_of_vk_group',
                        type=str,
                        help='https://vk.com/THIS <- "THIS" part of URL is a group name')
cli_parser.add_argument('-o',
                        metavar='offset',
                        type=int,
                        default=0,
                        required=False,
                        help='default=0. starting position relative to the latest post',
                        )
cli_parser.add_argument('-n',
                        metavar='number',
                        type=int,
                        default=50,
                        required=False,
                        help='default=50. how many posts will be downloaded',
                        )
cli_parser.add_argument('-c',
                        metavar='coroutines',
                        type=int,
                        default=2,
                        required=False,
                        help="default=2. limit for simultaneous coroutines. setting the value over ~5 "
                             "won't accelerate the process - bottleneck is Splash",
                        )
args = cli_parser.parse_args()

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(download_vk_group(args.name,
                                                                  offset=args.o,
                                                                  number=args.n,
                                                                  coroutines=args.c))
