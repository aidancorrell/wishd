"""The theme control.

Three states, not two: light, dark, and no stored choice at all. The third is
the one worth testing, because it is the state every new visitor is in and the
one a boolean toggle would quietly collapse.

There is no JavaScript (ADR-002), so the choice is a cookie the server stamps
onto <html>. That also means the theme is right in the first painted frame,
with none of the flash a script-applied theme has to work around.
"""

from __future__ import annotations

from dataspine import web


def test_no_choice_stamps_nothing(api_client):
    """With nothing stored, the stylesheet's media query must be left to decide."""
    html = api_client.get("/login").text
    assert 'data-theme' not in html
    assert '<html lang="en">' in html


def test_choosing_dark_stamps_the_document(api_client):
    api_client.post("/theme", data={"theme": "dark", "next": "/login"},
                    follow_redirects=False)
    html = api_client.get("/login").text
    assert 'data-theme="dark"' in html


def test_choosing_light_stamps_the_document(api_client):
    api_client.post("/theme", data={"theme": "light", "next": "/login"},
                    follow_redirects=False)
    assert 'data-theme="light"' in api_client.get("/login").text


def test_auto_clears_a_stored_choice(api_client):
    """Auto is a real state. Picking it must return to following the OS, not
    store whichever theme we happen to prefer."""
    api_client.post("/theme", data={"theme": "dark", "next": "/login"},
                    follow_redirects=False)
    assert 'data-theme="dark"' in api_client.get("/login").text

    api_client.post("/theme", data={"theme": "", "next": "/login"},
                    follow_redirects=False)
    assert "data-theme" not in api_client.get("/login").text


def test_an_unknown_theme_is_refused_rather_than_stored(api_client):
    """A hand-edited form must not store a theme no stylesheet block matches."""
    api_client.post("/theme", data={"theme": "solarized", "next": "/login"},
                    follow_redirects=False)
    assert "data-theme" not in api_client.get("/login").text


def test_the_choice_returns_you_to_the_page_you_were_on(api_client):
    resp = api_client.post("/theme", data={"theme": "dark", "next": "/monitors"},
                           follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/monitors"


def test_next_cannot_leave_the_site(api_client):
    """`next` arrives from a form field, so it must not become an open redirect."""
    for hostile in ("https://evil.example/x", "//evil.example/x", "javascript:alert(1)"):
        resp = api_client.post("/theme", data={"theme": "dark", "next": hostile},
                               follow_redirects=False)
        assert resp.headers["location"] == "/", f"{hostile} was allowed through"


def test_every_page_carries_the_control(api_client):
    for route in ("/", "/overview", "/monitors", "/catalog", "/incidents", "/costs", "/jobs"):
        html = api_client.get(route).text
        assert 'action="/theme"' in html, f"{route} has no theme control"


def test_theme_helper_ignores_a_junk_cookie(api_client):
    api_client.cookies.set(web.THEME_COOKIE, "neon")
    assert "data-theme" not in api_client.get("/login").text
    api_client.cookies.clear()
