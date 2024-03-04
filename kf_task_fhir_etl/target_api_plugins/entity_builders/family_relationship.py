"""
Builds FHIR Observation resources (https://www.hl7.org/fhir/observation.html)
from rows of tabular participant family relationship data.
"""
from abc import abstractmethod

from kf_lib_data_ingest.common import constants
from kf_lib_data_ingest.common.concept_schema import CONCEPT
from kf_task_fhir_etl.target_api_plugins.entity_builders import Patient
from kf_task_fhir_etl.common.utils import not_none, drop_none, yield_resource_ids

# http://hl7.org/fhir/ValueSet/observation-status
status_code = "final"

# http://terminology.hl7.org/ValueSet/v3-FamilyMember
code_coding = {
    "Aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "AUNT",
        "display": "aunt",
    },
    constants.RELATIONSHIP.BROTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "BRO",
        "display": "brother",
    },
    "Brother-in-law": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "BROINLAW",
        "display": "brother-in-law",
    },
    "Brother-Monozygotic Twin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWINBRO",
        "display": "twin brother",
    },
    constants.RELATIONSHIP.CHILD: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "CHILD",
        "display": "child",
    },
    "Cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "COUSN",
        "display": "cousin",
    },
    constants.RELATIONSHIP.DAUGHTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "DAUC",
        "display": "daughter",
    },
    "father": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "FTH",
        "display": "father",
    },
    constants.RELATIONSHIP.FATHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "FTH",
        "display": "father",
    },
    "First cousin once removed": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    constants.RELATIONSHIP.GRANDCHILD: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDCHILD",
        "display": "grandchild",
    },
    constants.RELATIONSHIP.GRANDDAUGHTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDDAU",
        "display": "granddaughter",
    },
    constants.RELATIONSHIP.GRANDFATHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRFTH",
        "display": "grandfather",
    },
    constants.RELATIONSHIP.GRANDMOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRMTH",
        "display": "grandmother",
    },
    constants.RELATIONSHIP.GRANDSON: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDSON",
        "display": "grandson",
    },
    "Great Nephew": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    constants.RELATIONSHIP.HUSBAND: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "HUSB",
        "display": "husband",
    },
    "Married in aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Married in Husband": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "HUSB",
        "display": "husband",
    },
    "Married in-Spouse": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SPS",
        "display": "spouse",
    },
    "Maternal aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MAUNT",
        "display": "maternal aunt",
    },
    "Maternal Aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MAUNT",
        "display": "maternal aunt",
    },
    "Maternal cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MCOUSN",
        "display": "maternal cousin",
    },
    "Maternal Cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MCOUSN",
        "display": "maternal cousin",
    },
    "Maternal grandfather": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRFTH",
        "display": "maternal grandfather",
    },
    constants.RELATIONSHIP.MATERNAL_GRANDDAUGHTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "GRNDDAU",
        "display": "granddaughter",
    },
    constants.RELATIONSHIP.MATERNAL_GRANDFATHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRFTH",
        "display": "maternal grandfather",
    },
    "Maternal grandmother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRMTH",
        "display": "maternal grandmother",
    },
    constants.RELATIONSHIP.MATERNAL_GRANDMOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGRMTH",
        "display": "maternal grandmother",
    },
    "Maternal great aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Great Aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Great Aunt (Mother's paternal aunt)": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Great Grandmother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MGGRMTH",
        "display": "maternal great-grandmother",
    },
    "Maternal Great Uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal half-sister": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal Relation": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Maternal uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MUNCLE",
        "display": "maternal uncle",
    },
    "mother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MTH",
        "display": "mother",
    },
    constants.RELATIONSHIP.MOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "MTH",
        "display": "mother",
    },
    "Nephew": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "NEPHEW",
        "display": "nephew",
    },
    "Niece": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "NIECE",
        "display": "niece",
    },
    "Paternal aunt": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PAUNT",
        "display": "paternal aunt",
    },
    "Paternal cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PCOUSN",
        "display": "paternal cousin",
    },
    "Paternal Cousin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PCOUSN",
        "display": "paternal cousin",
    },
    "Paternal grandfather": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PGRFTH",
        "display": "paternal grandfather",
    },
    "Paternal grandmother": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PGRMTH",
        "display": "paternal grandmother",
    },
    constants.RELATIONSHIP.PATERNAL_GRANDMOTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PGRMTH",
        "display": "paternal grandmother",
    },
    "Paternal uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "PUNCLE",
        "display": "paternal uncle",
    },
    constants.RELATIONSHIP.PROBAND: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "CHILD",
        "display": "child",
    },
    constants.RELATIONSHIP.SIBLING: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SIB",
        "display": "sibling",
    },
    constants.RELATIONSHIP.SISTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SIS",
        "display": "sister",
    },
    constants.RELATIONSHIP.SON: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SONC",
        "display": "son",
    },
    constants.RELATIONSHIP.SPOUSE: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "SPS",
        "display": "spouse",
    },
    "Twin": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWIN",
        "display": "twin",
    },
    constants.RELATIONSHIP.TWIN_BROTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWINBRO",
        "display": "twin brother",
    },
    constants.RELATIONSHIP.TWIN_SISTER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "TWINSIS",
        "display": "twin sister",
    },
    "Uncle": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "UNCLE",
        "display": "uncle",
    },
    "Uncle-married in": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "EXT",
        "display": "extended family member",
    },
    "Wife": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
        "code": "WIFE",
        "display": "wife",
    },
    constants.COMMON.OTHER: {
        "system": "http://terminology.hl7.org/CodeSystem/v3-NullFlavor",
        "code": "OTH",
        "display": "other",
    },
}


class FamilyRelationship:
    class_name = "family_relationship"
    api_path = "Observation"
    target_id_concept = None
    service_id_fields = None

    @classmethod
    def get_key_components(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.PROJECT.ID]
        assert not_none(
            get_target_id_from_record(
                Patient,
                {
                    CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                    CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: record[
                        CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID
                    ],
                },
            )
        )
        assert not_none(
            get_target_id_from_record(
                Patient,
                {
                    CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                    CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: record[
                        CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID
                    ],
                },
            )
        )
        assert not_none(record[CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2])

        return {
            "_tag": study_id,
            "identifier": not_none(
                record[CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID]
            ),
        }

    @classmethod
    def query_target_ids(cls, host, key_components):
        return list(yield_resource_ids(host, cls.api_path, drop_none(key_components)))

    @classmethod
    def build_entity(cls, record, get_target_id_from_record):
        study_id = record[CONCEPT.PROJECT.ID]
        family_relationship_id = record[CONCEPT.FAMILY_RELATIONSHIP.TARGET_SERVICE_ID]
        external_id = record.get(CONCEPT.FAMILY_RELATIONSHIP.ID)
        participant1_id = record[CONCEPT.FAMILY_RELATIONSHIP.PERSON1.TARGET_SERVICE_ID]
        participant2_id = record[CONCEPT.FAMILY_RELATIONSHIP.PERSON2.TARGET_SERVICE_ID]
        relation_from_1_to_2 = record[CONCEPT.FAMILY_RELATIONSHIP.RELATION_FROM_1_TO_2]

        entity = {
            "resourceType": cls.api_path,
            "id": get_target_id_from_record(cls, record),
            "meta": {
                "profile": [
                    "https://ncpi-fhir.github.io/ncpi-fhir-ig/StructureDefinition/family-relationship"
                ],
                "tag": [
                    {
                        "system": "https://kf-api-dataservice.kidsfirstdrc.org/studies/",
                        "code": study_id,
                    }
                ],
            },
            "identifier": [
                {
                    "use": "official",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/family-relationships/",
                    "value": family_relationship_id,
                }
            ],
            "status": status_code,
            "code": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-RoleCode",
                        "code": "FAMMEMB",
                        "display": "family member",
                    }
                ],
                "text": "Family Relationship",
            },
        }

        # identifier
        if external_id:
            entity["identifier"].append(
                {
                    "use": "secondary",
                    "system": "https://kf-api-dataservice.kidsfirstdrc.org/family_relationships?external_id=",
                    "value": external_id,
                }
            )

        # subject
        subject_id = get_target_id_from_record(
            Patient,
            {
                CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: participant1_id,
            },
        )
        entity["subject"] = {"reference": f"{Patient.api_path}/{subject_id}"}

        # focus
        focus_id = get_target_id_from_record(
            Patient,
            {
                CONCEPT.STUDY.TARGET_SERVICE_ID: study_id,
                CONCEPT.PARTICIPANT.TARGET_SERVICE_ID: participant2_id,
            },
        )
        entity.setdefault("focus", []).append(
            {"reference": f"{Patient.api_path}/{focus_id}"}
        )

        # valueCodeableConcept
        if relation_from_1_to_2:
            value = {"text": relation_from_1_to_2}
            if code_coding.get(relation_from_1_to_2):
                value.setdefault("coding", []).append(code_coding[relation_from_1_to_2])
            entity["valueCodeableConcept"] = value

        return entity

    @abstractmethod
    def submit(cls, host, body):
        pass
