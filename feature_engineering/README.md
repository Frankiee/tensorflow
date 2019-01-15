## Resources
* [Course Link](https://www.coursera.org/learn/feature-engineering/home/welcome)

## Week 1: Features
* [Code Link](https://github.com/Frankiee/training-data-analyst/tree/master/courses/machine_learning/deepdive/04_features)

[Python Notebook](https://github.com/Frankiee/training-data-analyst/blob/master/courses/machine_learning/deepdive/04_features/a_features.ipynb)

### To run add_features
* `cd feature_engineering`
* `python week1_1_features/add_features.py`

`housing_trained` folder is generated containing checkpoint & log data

### To view the result in Tensorboard
* `tensorboard --logdir ./housing_trained`
* Open http://localhost:6006/ in browser

## Week 1: GCP Dataflow
* [Code Link](https://github.com/Frankiee/training-data-analyst/tree/master/courses/data_analysis/lab2)

### To run the pipeline locally
* `python grep.py`

### To run the pipeline on the cloud
* If you don't already have a bucket on Cloud Storage, create one from the Storage section of the GCP console. Bucket names have to be globally unique.
* Copy some Java files to the cloud (make sure to replace <YOUR-BUCKET-NAME> with the bucket name you created in the previous step):
* `gsutil cp week1_2_dataflow/javahelp/*.java gs://<YOUR-BUCKET-NAME>/javahelp
* `python grepc.py`
