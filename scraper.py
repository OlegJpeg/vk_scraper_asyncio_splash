import re

import aiohttp
import asyncio

from bs4 import BeautifulSoup

from models import VkGroup, Post


async def download_vk_group(group_name, offset=0, post_range=10, coroutines=3):
    async with aiohttp.ClientSession() as session:
        url = f"https://vk.com/{group_name}"
        async with session.get(url) as r:
            # first call to group's url gets id & latest post's number
            assert r.status == 200, f'{r.status}: {r.reason}. url: {r.url}'
            html = await r.text()
            soup = BeautifulSoup(html, features="html.parser")
            element_title = soup.find('title').text
            name = re.compile(r'^(.*)( \| ВКонтакте)'). \
                search(element_title). \
                groups()[0]
            posts = soup.find_all('a', attrs={'class': 'post__anchor anchor'})
            post_numbers = []
            for post in posts:
                post_numbers.append(int(re.compile(r'(\d*)$').search(post['name']).group()))
            group_id = re.compile(r'post-(\d*)_').search(posts[0]['name']).groups()[0]
            latest_post = max(post_numbers)
            vk_group = VkGroup(group_id, name)

        start = latest_post - offset
        if post_range == 0:
            end = 1
        else:
            end = start - post_range

        tasks = []
        for post_id in range(start, end, -1):
            url = f'wall-{group_id}_{post_id}'
            task = asyncio.create_task(download_post(session, url, vk_group))
            tasks.append(task)
        await gather_with_concurrency(*tasks, n=coroutines)
    vk_group.save_to_csv()
    print(f'Posts saved: {len(vk_group.posts)}')


async def download_post(session, url, vk_group):
    # requests are made via Splash to have JS rendered
    async with session.get(f'http://127.0.0.1:8050/render.html',
                           params={
                               'url': f'https://vk.com/{url}',
                               'wait': 5,
                               'viewport': '1920x1080',
                               'images': 0,
                           }) as r:
        if not r.status == 200:
            print(f'{r.status}: {r.reason}. url: {r.url}')
            return
        html = await r.text()
        soup = BeautifulSoup(html, features="html.parser")
        # 'message_page_body' means error notification like "post was deleted"
        if soup.find('div', class_='message_page_body'):
            return
        post_url = soup.find('meta', attrs={'property': 'og:url'}).attrs['content']
        # check if it is post or a comment - they are numbered together, with no distinction
        if post_url != f'https://vk.com/{url}':
            return
        # page parsing happens in Post.__init__() method
        post = Post(soup)
        await post.save_images(session)
        vk_group.posts.append(post)


async def gather_with_concurrency(*tasks, n):
    # semaphore limits number of simultaneous coroutines
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(download_vk_group('pitchfork_rus',
                                                                  offset=0,
                                                                  post_range=10,
                                                                  coroutines=2))
