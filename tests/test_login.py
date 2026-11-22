import httpx
import pytest

from teehr import Client
from teehr.client import TeehrError
from teehr.login import SignInFailed, sign_in
from tests.conftest import API


def test_sign_in_polls_until_approved(api):
    shown = []
    made = sign_in(Client(API), "nb", show=shown.append, open_browser=False)
    assert made["token"].startswith("tpat_") and api.polls == 2
    assert any("ABCD-EFGH" in line for line in shown)


def test_sign_in_gives_up_when_not_approved(api):
    now = [0.0]

    def sleep(seconds):
        now[0] += seconds

    api.auto_approve = False
    with pytest.raises(SignInFailed):
        sign_in(Client(API), "nb", show=lambda _: None, open_browser=False,
                sleep=sleep, clock=lambda: now[0])  # fmt: skip
    assert not api.approved


def test_an_http_url_that_redirects_says_which_url_to_use():
    def handle(request):
        url = str(request.url).replace("http://", "https://")
        return httpx.Response(301, headers={"Location": url})

    http = httpx.Client(transport=httpx.MockTransport(handle))
    with pytest.raises(TeehrError) as caught:
        Client("http://cp.example", http=http).start_sign_in("nb")
    assert caught.value.status == 301
    assert caught.value.detail == (
        "http://cp.example redirects to https://cp.example: use that as the API URL (--api)"
    )
