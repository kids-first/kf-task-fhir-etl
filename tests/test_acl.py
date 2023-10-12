
from kf_task_fhir_etl.target_api_plugins.common import _set_authorization


def test_set_auth():
    """
    Test _set_authorization method which formats ACL values
    """
    gf = {
        "acl": [ "*" ],
        "authz": []
    }
    assert _set_authorization(gf) == ["*"]

    gf = {
        "acl": [ ],
        "authz": [ "/programs/SD_11111111" ],
    }
    assert _set_authorization(gf) == ["SD_11111111"]

    gf = {
        "acl": [],
        "authz": [ "/open" ],
    }
    assert _set_authorization(gf) == ["*"]

