import tensorflow as tf
import tensorflow_hub as hub

embed = hub.load("E:\Semantic search\\universal-sentence-encoder_4\\")
embeddings = tf.make_ndarray(tf.make_tensor_proto(embed(["The quick brown fox jumps over the lazy dog."]))).tolist()[0]

print(type(embeddings))
print(len(embeddings))
print(embeddings)