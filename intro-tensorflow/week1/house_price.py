import tensorflow as tf
import shutil

shutil.rmtree("outdir", ignore_errors=True)     # start fresh each time


def train_input_fn():
    features = {
        "sq_footage": [
            1000,
            2000,
            3000,
            1000,
            2000,
            3000,
        ],
        "type": [
            "house",
            "house",
            "house",
            "apt",
            "apt",
            "apt",
        ],
    }

    labels = [
        500,
        1000,
        1500,
        700,
        1300,
        1900,
    ]   # in thousands

    return features, labels


featcols = [
    tf.feature_column.numeric_column("sq_footage"),
    tf.feature_column.categorical_column_with_vocabulary_list(
        "type",
        ["house", "apt"],
    )
]

model = tf.estimator.LinearRegressor(featcols, "outdir")

model.train(train_input_fn, steps=2000)


def predict_input_fn():
    features = {
        "sq_footage": [
            1500,
            1500,
            2500,
            2500,
        ],
        "type": [
            "house",
            "house",
            "apt",
            "apt",
        ]
    }

    return features


predictions = model.predict(predict_input_fn)

print next(predictions)
print next(predictions)
print next(predictions)
print next(predictions)
