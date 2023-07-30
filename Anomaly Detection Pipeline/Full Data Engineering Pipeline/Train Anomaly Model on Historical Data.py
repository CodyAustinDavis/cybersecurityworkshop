# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Build Training / Test Set on Up-to-date historical feature data

# COMMAND ----------

# DBTITLE 1,Thin wrapper library to wrap PyOD algorithms with ML Flow
# MAGIC %pip install databricks-kakapo

# COMMAND ----------


from hyperopt import fmin, tpe, hp, Trials, SparkTrials, STATUS_OK
from hyperopt.pyll.base import scope
from pyspark.sql.functions import struct, col, when, array
from pyspark.sql.types import DoubleType
from pyod.utils.data import generate_data

import mlflow
import kakapo
import uuid
import sys


# COMMAND ----------

# DBTITLE 1,Set Database / Catalog Scope for Session
spark.sql("USE cyberworkshop")

# COMMAND ----------

# DBTITLE 1,Build Training / Test Data Frame
historical_df = spark.sql("""
                          -- Historical data aggregates
                SELECt 
                      nn.id,
                      AvgLoginAttemptsPerDay,
                      AvgFailedAttemptsPerDay,
                      NumTimesWrittenToSysFolderPerDay,
                      NumTimesDeletedSysFolderPerDay,
                      NumTimesAccessedSysFolderPerDay,
                      NumTimesAccessedNormalFolderPerDay,
                      NumTimesDeletedNormFolderPerDay,
                      HasSharedFiles,
                      NumUsersSharingWithPerDay,
                      NumFilesSharedPerDay,
                      NumFailedFileSharesPerDay,
                      NumFailedShareReadAttemptsPerDay
                      FROM prod_silver_nodes nn
                      -- Get Login Features
                      LEFT JOIN prod_gold_user_historical_features_logins lf ON lf.user_join_key = nn.id
                      -- Get file access patterns
                      LEFT JOIN prod_gold_user_historical_features_file_access_patterns fp ON fp.user_join_key = nn.id
                      -- Get data sharing patterns
                      LEFT JOIN prod_gold_user_historical_features_sharing_patterns AS ds ON ds.user_join_key = nn.id
                      WHERE nn.entity_type = 'user'
                                                
                          """)


# COMMAND ----------

# DBTITLE 1,Train / Test Split - Unsupervised
from sklearn.model_selection import train_test_split
from pyspark.ml.feature import VectorAssembler

#vecAssembler = VectorAssembler(outputCol="features")
#vecAssembler.setInputCols([c for c in historical_df.columns if c != "id"])

## We dont need Vec assembler here since the model does this for us vecAssembler.transform(
historical_features = historical_df.fillna(0).toPandas().set_index("id")

X_train, X_test = train_test_split(historical_features, test_size=0.1)


X_train_ids = X_train.index
X_test_ids = X_test.index

print(f"Training data count: {X_train.count()}")
print(f"Test data count: {X_test.count()}")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Define Experiment Scope
user = spark.sql("select current_user()").take(1)[0][0]

mlflow.set_experiment(f"/Users/{user}/uba_models")

experiment_id = mlflow.get_experiment_by_name(f"/Users/{user}/uba_models").experiment_id
print(experiment_id)

# COMMAND ----------

# DBTITLE 1,Single Model Training Example
import pandas as pd
import numpy as np
from pyod.models.knn import KNN

## Pick any model, this is KNN for anomaly detection
model = KNN()

model.fit(X_train)

anomaly_scores = model.decision_function(X_train)

predictions_df = pd.DataFrame({"id": X_train_ids, "AnomalyScore": anomaly_scores})

display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Now scale out model selecting + hyper-parameter tuning with HyperOpt + SparkTrials

# COMMAND ----------

# DBTITLE 1,Define Hyper-parameter search space

# Load default model space
model_space = kakapo.get_default_model_space()
print("Default model space: {}".format(model_space))


# Load default hyper param search space
search_space = kakapo.get_default_search_space()
print("Default search space: {}".format(search_space))


# Load search space into hyperopt
space = hp.choice('model_type', search_space)

## Whether or not this model is supervised
GROUND_TRUTH_OD_EXISTS = False

# Unique run ID when saving MLFlow experiment
uid = uuid.uuid4().hex



# COMMAND ----------

# DBTITLE 1,What is inside the Kakapo library? An easier wrapper for PyOD algorithms + ML Flow

def train_outlier_detection_example(params, model_space, X_train, X_test, y_test, ground_truth_flag):
    """
    Train an outlier detection model using the pyfunc wrapper for PyOD algorithms and log into mlflow
    """
    mlflow.autolog(disable=False)
    mlflow.set_tag("model_type", params["type"])

    model = PyodWrapper(**params)
    model.set_model_space(model_space)
    model.fit(X_train)

    y_train_pred = model.predict(None, X_train)

    # Get model input and output signatures
    model_input_df = X_train
    model_output_df = y_train_pred
    model_signature = infer_signature(model_input_df, model_output_df)

    # log our model to mlflow
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=model,
        signature=model_signature
    )

    if (ground_truth_flag):
        y_test_pred = model.predict(None, X_test)
        score = roc_auc_score(y_score=y_test_pred, y_true=y_test)
    else:
        score = emmv_scores(model, X_test)["em"]

    return {'loss': -score, 'status': STATUS_OK}

# COMMAND ----------

# DBTITLE 1,Run parallel hyper-parameter tuning
from kakapo import train_outlier_detection
from kakapo import get_default_model_space
from kakapo import get_default_search_space


spark_trials = SparkTrials(parallelism=10)

with mlflow.start_run():
  best_params = fmin(
    trials=spark_trials,
    fn = lambda params: train_outlier_detection(params, model_space, X_train, X_test, None, GROUND_TRUTH_OD_EXISTS),
    space = space,
    algo = tpe.suggest,
    max_evals = 50
  )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What can we do with all these models now?
# MAGIC
# MAGIC - Dynamically pick the best one and push to production
# MAGIC - Choose the best run for each model type and create a prediction function to generate ensemble predictions

# COMMAND ----------

# DBTITLE 1,Simple Approach - Get best run and promote to production model

metric = "loss"

# Get all child runs on current experiment
runs = mlflow.search_runs(experiment_ids=experiment_id, order_by=[f'metrics.{metric} ASC'])
runs = runs.where(runs['status'] == 'FINISHED')

# Get best run id and logged model
best_run_id = runs.loc[0,'run_id']
logged_model = f'runs:/{best_run_id}/model'

print(f"Best Run to promote: {best_run_id}")
print(f"Mode to promote: {logged_model}")

# COMMAND ----------


import mlflow
import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

# The default path where the MLflow autologging function stores the TensorFlow Keras model
model_name = "uba_amomaly_model"
artifact_path = "model"

model_details = mlflow.register_model(model_uri=logged_model, name=model_name)


# Wait until the model is ready
def wait_until_ready(model_name, model_version):
  client = MlflowClient()
  for _ in range(10):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(1)


wait_until_ready(model_details.name, model_details.version)

# COMMAND ----------

# DBTITLE 1,Promote to Production - Can Integrate with any CI/CD process
client = MlflowClient()

client.transition_model_version_stage(
  name=model_details.name,
  version=model_details.version,
  stage='production',
)
model_version_details = client.get_model_version(
  name=model_details.name,
  version=model_details.version,
)
print("The current model stage is: '{stage}'".format(stage=model_version_details.current_stage))

latest_version_info = client.get_latest_versions(model_name, stages=["production"])
latest_production_version = latest_version_info[0].version
print("The latest production version of the model '%s' is '%s'." % (model_name, latest_production_version))

# COMMAND ----------

client.get_latest_versions(model_name, stages = ["Production"])

# COMMAND ----------

# DBTITLE 1,Load Model - Can be done in later notebooks / jobs
  from mlflow.tracking.client import MlflowClient
  client = MlflowClient()
  model_stage = ["Production"]
  model_version = client.get_latest_versions(model_name, stages=model_stage)[0].version
  model_uri = "models:/{model_name}/{model_stage}".format(model_name=model_name, model_stage=model_stage[0])
  prod_model = mlflow.pyfunc.load_model(model_uri)

  print(f"Loading model {model_name} - {model_stage[0]} to generate anomaly predictions...")
