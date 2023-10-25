
import os
import shutil


def write_study_tables(merged_df_dict, data_dir):
    """
    Write output of extract or transform stage to file

    Structure:
        data/<study kf id>
          - Participant.csv
          - Biospecimen.csv
          ...
    """
    shutil.rmtree(data_dir, ignore_errors=True)
    for study_id, df_dict in merged_df_dict.items():
        study_dir = os.path.join(data_dir, study_id)
        os.makedirs(study_dir, exist_ok=True)
        for entity_type, df in df_dict.items():
            fp = os.path.join(study_dir, f"{entity_type}.csv")
            df.to_csv(fp)
            logging.info(
                f"✏️  Wrote {entity_type} transform df to {fp}"
            )
