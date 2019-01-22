## Resources
* [Course Link](https://www.coursera.org/learn/sequence-models-tensorflow-gcp/home/welcome)

## Week 2: Sequence Models for Time Series and Natural Language Processing
* [Code Link](https://github.com/Frankiee/training-data-analyst/blob/master/courses/machine_learning/deepdive/09_sequence)

[Python Notebook](https://github.com/Frankiee/training-data-analyst/blob/master/courses/machine_learning/deepdive/09_sequence/poetry.ipynb)

### To pull the dataset
* `cd sequence-models-tensorflow-gcp`
* `python week2_encoder_decoder/poem_encoder_decoder.py`

### Our problem (translation) requires the creation of text sequences from the training dataset
```bash
DATA_DIR=./t2t_data
TMP_DIR=$DATA_DIR/tmp
rm -rf $DATA_DIR $TMP_DIR
mkdir -p $DATA_DIR $TMP_DIR
t2t-datagen \
  --t2t_usr_dir=week2_encoder_decoder/trainer \
  --problem=poetry_line_problem \
  --data_dir=$DATA_DIR \
  --tmp_dir=$TMP_DIR
```

### Train model locally
```bash
mkdir t2t_data/subset
cp t2t_data/poetry_line_problem-train-00080-of-00090 t2t_data/subset/
cp t2t_data/poetry_line_problem-dev-00000-of-00010  t2t_data/subset/
cp t2t_data/vocab.poetry_line_problem.8192.subwords t2t_data/subset/

OUTDIR=./trained_model
rm -rf $OUTDIR
t2t-trainer \
  --data_dir=t2t_data/subset \
  --t2t_usr_dir=week2_encoder_decoder/trainer \
  --problem=poetry_line_problem \
  --model=transformer \
  --hparams_set=transformer_poetry \
  --output_dir=$OUTDIR --job-dir=$OUTDIR --train_steps=10
```