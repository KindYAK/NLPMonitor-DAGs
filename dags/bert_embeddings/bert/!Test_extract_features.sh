python extract_features.py \
  --input_file=input.txt \
  --output_file=output.json \
  --vocab_file=../rubert_cased_L-12_H-768_A-12_v1/vocab.txt \
  --bert_config_file=../rubert_cased_L-12_H-768_A-12_v1/bert_config.json \
  --init_checkpoint=../rubert_cased_L-12_H-768_A-12_v1/bert_model.ckpt \
  --layers=-2 \
  --max_seq_length=128 \
  --batch_size=1000
