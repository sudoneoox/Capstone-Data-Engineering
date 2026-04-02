"""
One-time download you need your kaggle API key in order to run this or you could also
manually download it by going to the url on the Kaggle website


"""

import subprocess

base_url: str = "https://www.kaggle.com/datasets"
sources: list[str] = [
    "asaniczka/1-3m-linkedin-jobs-and-skills-2024",
    "asaniczka/linkedin-data-engineer-job-postings",
    "asaniczka/all-jobs-on-upwork-200k-plus",
    "asaniczka/upwork-job-postings-dataset-2024-50k-records",
    "asaniczka/data-analyst-job-postings",
    "asaniczka/software-engineer-job-postings-linkedin",
    "asaniczka/data-science-job-postings-and-skills",
    "arshkon/linkedin-job-postings",
    "ravindrasinghrana/job-description-dataset",
    "ravindrasinghrana/employeedataset",
]


def ingest_kaggle():

    for source in sources:
        subprocess.run(
            [
                "kaggle",
                "datasets",
                "download",
                "-d",
                source,
                "-p",
                f"dbt/seeds/kaggle/{source.split('/')[1]}",
            ]
        )
