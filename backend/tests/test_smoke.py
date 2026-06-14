"""Smoke test base — verifica che pytest + conftest funzionano."""

def test_pytest_works():
    assert 1 + 1 == 2


def test_fake_mt5(fake_mt5):
    assert fake_mt5.initialize()
    info = fake_mt5.account_info()
    assert info.balance == 10000.0
