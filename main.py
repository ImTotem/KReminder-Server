from bs4 import BeautifulSoup as bs
import requests

from datetime import datetime
import firebase_admin
from firebase_admin import credentials, db, messaging
import inko

from time import sleep

myInko = inko.Inko()

BULLETIN = {
    14: "일반공지",
    15: "장학공지",
    16: "학사공지",
    21: "학생생활",
    22: "자유게시판",
    150: "채용공지",
    151: "현장실습공지",
    191: "사회봉사공지",  # 로그인 필요
}

LAST_POST_ID = "lastPostID"
KEYWORDS = "keywords"

MAX_PAGE_SIZE = 30


class Post(object):
    def __init__(self, post_id: int = None, bulletin: int = None, title=""):
        self.post_id = post_id
        self.bulletin = bulletin
        self.title = title

    def __str__(self):
        return str({
            "post_id": self.post_id,
            "bulletin": self.bulletin,
            "title": self.title
        })

    def __lt__(self, other):
        return self.post_id < other.post_id if isinstance(other, Post) else False

    def __gt__(self, other):
        return self.post_id > other.post_id if isinstance(other, Post) else False

    def __le__(self, other):
        return self.post_id <= other.post_id if isinstance(other, Post) else False

    def __ge__(self, other):
        return self.post_id >= other.post_id if isinstance(other, Post) else False

    def __eq__(self, other):
        return self.post_id == other.post_id if isinstance(other, Post) else False

    def __ne__(self, other):
        return self.post_id != other.post_id if isinstance(other, Post) else False


def set_credentials():
    cred_path = "k-reminder-firebase-adminsdk-c05d7-1377c1e5c6.json"
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred, {
        'databaseURL': ""
    })


def get_keywords():
    ret = []
    db_keyword = ref.child(KEYWORDS)
    snapshot = db_keyword.get()
    for key, value in snapshot.items():
        # 키워드 조회하는 김에 구독자 수가 1이하 삭제
        if int(value) < 1:
            db_keyword.child(key).delete()
            print(f"[ {key} ] 가 삭제되었습니다: {value}")
        else:
            ret.append(key)

    return ret


def get_last_post_id(bulletin):
    return ref.child(LAST_POST_ID).child(str(bulletin)).get()


def set_last_post_id(bulletin, post_id):
    ref.child(LAST_POST_ID).update({str(bulletin): post_id})


def send_message(keyword, post: Post) -> str:
    # 한글은 키워드로 설정할 수 없음 -> 한영변환
    keyword = myInko.ko2en(keyword)

    # 전송할 데이터
    data_message = {
        "post_id": post.post_id,
        "bulletin": post.bulletin,
        "title": post.title,
    }

    message = messaging.Message(
        # 구독한 사용자에게만 알림 전송
        topic=keyword,
        data=data_message,
    )

    response = messaging.send(message)
    return response


def send_all(topics, post: list[Post]):
    ret = []
    try:
        for t in topics:
            for p in post:
                if t in p:
                    ret.append(send_message(t, p))
    except Exception as e:
        exception_collector.append(e)
    finally:
        return ret


def send_error_message(message):
    return send_message(
        "모니터링키워드",
        Post(title=f"{datetime.now().isoformat()}, {message}")
    )


def crawling(bulletin) -> list[Post]:
    request_url = f"https://portal.koreatech.ac.kr/ctt/bb/bulletin?b={bulletin}&ls={MAX_PAGE_SIZE}"

    ret_posts = []
    try:
        response = requests.get(request_url)
        soup = bs(response.text, "html.parser")
    except Exception as e:
        print("HTTP 요청 실패")
        send_error_message("HTTP 요청 실패")
        exception_collector.append(e)
    else:
        html_posts = soup.select("#boardTypeList > tbody > tr[data-url]")

        if not html_posts:
            print("공지 리스트 api 주소 확인 필요")
            send_error_message("공지 리스트 api 주소 확인 필요")

        for i in range(min(len(html_posts), MAX_PAGE_SIZE)):
            try:
                post_id = int(html_posts[i].select_one("td.bc-s-post_seq").text.strip())
                title = html_posts[i].select_one("td.bc-s-title > div > span[title]")["title"].strip()

                ret_posts.append(Post(post_id=post_id, bulletin=bulletin, title=title))

            except Exception as e:
                exception_collector.append(e)

    finally:
        return ret_posts


def filtering(bulletin, raw):
    last_post_id = get_last_post_id(bulletin)
    last_post_id = [last_post_id, 0][last_post_id is None]

    ret = sorted(filter(lambda x: last_post_id < x.post_id, raw))

    return last_post_id, tuple(ret)


if __name__ == "__main__":
    exception_collector = []

    weekday = datetime.today().weekday()
    now = datetime.now()
    hour = now.strftime("%H")
    if 0 <= weekday <= 4 and 8 <= int(hour) <= 19:  # 월~금, 8시~7시 사이에만 작동
        set_credentials()

        ref = db.reference()
        keywords = get_keywords()

        print("-----------------------------------------------")

        print(f"Date: {now.isoformat()}")

        for b, category in BULLETIN.items():
            posts: list[Post] = crawling(b)

            prev_post_id, posts = filtering(b, posts)

            res = send_all(keywords, posts)

            if posts:
                set_last_post_id(b, posts[-1].post_id)

            log = {
                "Prev Post Id": prev_post_id,
                "Now Post Id": posts[-1].post_id if posts else prev_post_id,
                "New Post": len(posts)
            }
            print(f"{category}({b}): {log}")
            print(*map(lambda p: f"[{BULLETIN[p.bulletin]}] {p.post_id} / {p.title}", posts), sep="\n")
            print("Send Result:")
            print(*res, sep="\n")

            sleep(0.5)  # Avoid omissions when updating data to a realtime database

        print("-----------------------------------------------")

    print(*exception_collector, sep="\n\n")
