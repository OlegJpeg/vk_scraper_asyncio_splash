import csv
import datetime
import os
import re


class Post:
    def __init__(self, soup, vk_group):
        self.group_name = vk_group.name
        self.group_id = vk_group.id
        self.number = self._number(soup)
        self.content = self._content(soup)
        self.date_published = self._date_published(soup)
        self.date_downloaded = datetime.datetime.now()
        self.views = self._views(soup)
        self.likes = self._likes(soup)
        self.reposts = self._reposts(soup)
        self.comments = self._comments(soup)
        self.link = self._link(soup)
        self.album = self._album(soup)
        self.has_source = self._has_source(soup)
        self.repost_from = self._repost_from(soup)
        self.images = self._images(soup)

    @staticmethod
    def _number(soup):
        number_element = soup.find(href=re.compile(r'/wall-(\d*)_(\d*)')).attrs['href']
        number = re.compile(r'/wall-(\d*)_(\d*)').search(number_element).groups()[1]
        return number

    @staticmethod
    def _content(soup):
        try:
            content = soup.find(class_='wall_post_text').prettify()
            content = re.sub(r'<.+>', '', content)
            content = re.sub(r'(&laquo;)|(&raquo;)', '"', content) + '\n\n'
            return content
        except AttributeError:
            return ''

    @staticmethod
    def _date_published(soup):
        timestamp = re.compile(r'statsMeta:\s{.*\"time\":(\d*),').search(str(soup)).groups()[0]
        return datetime.datetime.fromtimestamp(int(timestamp))

    @staticmethod
    def _views(soup):
        try:
            views_string = soup.find(class_="like_views _views").text
        except AttributeError:
            return '0'
        views = re.compile(r'^\d*').search(views_string).group()
        if views == '':
            return '0'
        elif views_string.endswith('K'):
            return str(int(views) * 1000)
        else:
            return str(int(views))

    @staticmethod
    def _likes(soup):
        try:
            return soup.find('a', class_='like').attrs['data-count']
        except AttributeError:
            return '0'

    @staticmethod
    def _reposts(soup):
        try:
            return soup.find('a', class_='share').attrs['data-count']
        except AttributeError:
            return '0'

    @staticmethod
    def _comments(soup):
        try:
            comments = soup.find(class_="replies_list").find_all('div', class_='reply_wrap')
            return str(len(comments))
        except AttributeError:
            return '0'

    @staticmethod
    def _link(soup):
        try:
            link = soup.find('a', class_="media_link__title")['href']
            return 'https://vk.com' + link
        except TypeError:
            return

    @staticmethod
    def _has_source(soup):
        if soup.find('div', class_="Post__copyright"):
            return True
        else:
            return False

    @staticmethod
    def _album(soup):
        if soup.find('a', attrs={'class': 'page_post_thumb_album'}):
            return True
        else:
            return False

    @staticmethod
    def _repost_from(soup):
        try:
            repost_from = soup.find("a", class_="copy_post_image").attrs['data-post-id']
            return 'https://vk.com/wall' + repost_from
        except AttributeError:
            return

    @staticmethod
    def _images(soup):
        images = []
        div_with_images = soup.find('div', class_='wall_text')
        regex_image_links = re.compile(r'https:.*?&type=album')
        regex_name = re.compile(r'(.*)/(.*)\?size=')
        try:
            for a in div_with_images.find_all(onclick=re.compile(r'^return\sshowPhoto')):
                url_raw = regex_image_links.findall(a.attrs['onclick'])[-1]
                url = re.sub(r'\\/', '/', url_raw)
                name = regex_name.search(url).groups()[1]
                image = {'url': url, 'name': name}
                images.append(image)
            return images
        except AttributeError:
            return

    async def save_images(self, session):
        # saving images to a folder
        if self.images:
            os.makedirs(os.path.join(f'{self.group_name} - {self.group_id}', 'files'), exist_ok=True)
            for image in self.images:
                async with session.get(image['url']) as r:
                    assert r.status == 200, f'{r.status}: {r.reason}. url: {r.url}'
                    pic = await r.read()
                    with open(os.path.join(f'{self.group_name} - {self.group_id}',
                                           'files',
                                           image['name']), 'wb') as file:
                        file.write(pic)


class GroupManager:
    def __init__(self, soup, target_number):
        self.id = self._id(soup)
        self.name = self._name(soup)
        self.latest_post = self._latest_post(soup)
        self.target_number = target_number
        self.error_504 = []
        self.posts = []

    @staticmethod
    def _id(soup):
        element = soup.find('a', attrs={'class': 'wi_date'}).attrs['href']
        return re.compile(r'wall-(\d*)_').search(element).groups()[0]

    @staticmethod
    def _name(soup):
        element = soup.find('h2', attrs={'class': 'basisGroup__groupTitle op_header'})
        return element.text.strip()

    @staticmethod
    def _latest_post(soup):
        posts = soup.find_all('a', attrs={'class': 'post__anchor anchor'})
        post_numbers = []
        for post in posts:
            post_numbers.append(int(re.compile(r'(\d*)$').search(post['name']).group()))
        return max(post_numbers)

    def save_to_csv(self):
        os.makedirs(f'{self.name} - {self.id}', exist_ok=True)
        with open(os.path.join(f'{self.name} - {self.id}', 'data.csv'), 'w',
                  newline='',
                  encoding='utf-8') as file:
            output_writer = csv.writer(file)
            for post in self.posts[:self.target_number]:
                # getting values of all attributes of Post, then writing them in file
                data = [value for value in vars(post).values()]
                output_writer.writerow(data)
