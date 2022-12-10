import pytest
from spai.optim.models import Settings, KPI, Purpose, Mode, Performance


@pytest.mark.parametrize('kpi', list(KPI))
@pytest.mark.parametrize('purpose', list(Purpose))
@pytest.mark.parametrize('mode', list(Mode))
def test_settings(kpi, purpose, mode):
    settings = Settings(kpi, purpose, mode, 100, 100, kpi, True)
    p = Performance(*([0] * 9), None)
    assert not settings.is_optimise_target([p])
