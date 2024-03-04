
import pytest
from kf_task_fhir_etl.target_api_plugins import common
from kf_task_fhir_etl.config import (
    DCF_BASE_URL
)


@pytest.mark.parametrize(
    "urls, dcf_id_exists",
    [
        (
            [
                "https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/aacf49ee-f3b5-4bf9-865c-2c26f5fb7890"
            ],
            True,
        ),
        (
            [
                "https://nci-crdc.datacommons.io/index/index/aacf49ee-f3b5-4bf9-865c-2c26f5fb7890"
            ],
            True,
        ),
        (
            [
                "https://nci-crdc.datacommons.io/user/aacf49ee-f3b5-4bf9-865c-2c26f5fb7890"
            ],
            False,
        ),
        (["s3://bucket/myfile.g.vcf.gz"], False),
    ],
)
def test_get_dcf_id(urls, dcf_id_exists):
    """
    Test _get_dcf_id
    """
    dcf_id = common._get_dcf_id(urls)
    if dcf_id_exists:
        assert dcf_id
    else:
        assert dcf_id is None


@pytest.mark.parametrize(
    "url, drs_uri",
    [
        ("https://kfgen3.org/index/index/foo1", "drs://kfgen3.org/foo1"),
        ("https://data.org/asdf/blah", "drs://data.org/blah"),

    ])
def test_drs_uri(url, drs_uri):
    """
    Test _drs_uri
    """
    assert common._drs_uri(url) == drs_uri


def test_update_dcf_file_metadata(mocker):
    """
    Test update_gf_metadata with DCF file
    """
    # Test setup
    input_gf = {
        "acl": [
            "phs002589.c1",
            "phs002589.c999",
            "SD_JK4Z4T6V"
        ],
        "authz": [
            "/programs/phs002589.c1"
        ],
        "availability": "Immediate Download",
        "controlled_access": True,
        "data_type": "Genome Aligned Read",
        "external_id": "aacf49ee-f3b5-4bf9-865c-2c26f5fb7890",
        "file_format": "bam",
        "file_name": "434893d2-b69c-4d16-a2d2-4ec6f935edff.Aligned.out.sorted.bam",
        "hashes": {
            "md5": "9fe8fd8c130f5b95eda33709a97972c1",
            "sha256": "ad92c21c32596a6f29ae3450063c52a2d313ab4682a0feb8793553729687661d"
        },
        "is_harmonized": True,
        "kf_id": "GF_7CWXJ5X4",
        "latest_did": "93b99c8e-c723-41e6-9e88-f75e42410cf3",
        "metadata": {},
        "paired_end": None,
        "reference_genome": "GRCh38",
        "size": 15177761597,
        "urls": [
            "https://nci-crdc.datacommons.io/ga4gh/drs/v1/objects/dcf-id"
        ],
    }
    mock_dcf_resp = {
        key: input_gf[key] for key in [
            "size", "hashes", "file_name", "acl", "authz"
        ]
    }
    mock_dcf_resp["id"] = "dcf-id"
    mock_dcf_resp["urls"] = (
        "s3://dcf_bucket/myfile"
    )

    # Patch funct and run func
    mocker.patch(
        "kf_task_fhir_etl.target_api_plugins.common.get_dcf_file"
    ).return_value.json.return_value = mock_dcf_resp
    output_gf = common.update_gf_metadata(input_gf)

    # Checkout outputs
    assert output_gf[common.DRS_URI_KEY] == f"drs://nci-crdc.datacommons.io/dcf-id"
    for key in ["size", "hashes", "file_name", "acl", "authz"]:
        assert output_gf[key] == input_gf[key]
    assert output_gf["urls"] == mock_dcf_resp["urls"]


def test_update_kf_file_metadata(mocker):
    """
    Test update_gf_metadata with kf file
    """
    # Test setup
    input_gf = {
        "acl": [],
        "authz": [],
        "availability": "Immediate Download",
        "controlled_access": True,
        "data_type": "Genome Aligned Read",
        "external_id": "aacf49ee-f3b5-4bf9-865c-2c26f5fb7890",
        "file_format": "bam",
        "file_name": "434893d2-b69c-4d16-a2d2-4ec6f935edff.Aligned.out.sorted.bam",
        "hashes": {
            "md5": "9fe8fd8c130f5b95eda33709a97972c1",
            "sha256": "ad92c21c32596a6f29ae3450063c52a2d313ab4682a0feb8793553729687661d"
        },
        "is_harmonized": True,
        "kf_id": "GF_7CWXJ5X4",
        "latest_did": "kf-did",
        "metadata": {},
        "paired_end": None,
        "reference_genome": "GRCh38",
        "size": 15177761597,
        "urls": [
            "s3://mybucket/myfile.cram"
        ],
    }

    # Patch funct and run func
    mocker.patch(
        "kf_task_fhir_etl.target_api_plugins.common.get_dcf_file"
    )
    output_gf = common.update_gf_metadata(input_gf)

    # Checkout outputs
    assert output_gf[common.DRS_URI_KEY] == f"drs://data.kidsfirstdrc.org/kf-did"
    for key in ["size", "hashes", "file_name", "urls"]:
        assert output_gf[key] == input_gf[key]
