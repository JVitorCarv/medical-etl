import kaggle

dataset = "gabrielsantello/medical-malpractice-insurance-dataset"
download_path = "./raw_data"
kaggle.api.dataset_download_files(dataset, path=download_path, unzip=True)
