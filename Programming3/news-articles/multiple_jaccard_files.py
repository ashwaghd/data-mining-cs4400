import json
import logging
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

# Regular expression to tokenize words
WORD_RE = re.compile(r"[\w']+")

class MRJaccardSimilarity(MRJob):

    def mapper_raw(self, input_path, input_uri):
        try:
            # Read the entire input file
            with open(input_path, 'r', encoding='utf-8') as f:
                json_data = f.read()
            # Parse the JSON array
            articles_json = json.loads(json_data)
            articles = []
            for article in articles_json:
                content = article.get("Content", "").lower()
                article_id = article.get("Title", "unknown")
                words = set(WORD_RE.findall(content))
                articles.append((article_id, words))
            # Now generate all pairs and compute Jaccard similarity
            for (id1, words1), (id2, words2) in combinations(articles, 2):
                intersection = words1 & words2
                union = words1 | words2
                if union:
                    jaccard_similarity = len(intersection) / len(union)
                else:
                    jaccard_similarity = 0
                # Yield the pair and their Jaccard similarity
                yield (id1, id2), jaccard_similarity
        except Exception as e:
            logging.error(f"Error in mapper_raw: {e}")
            raise

    def reducer(self, article_pair, similarities):
        try:
            # Since each pair has only one similarity, yield it
            for similarity in similarities:
                yield article_pair, similarity
        except Exception as e:
            logging.error(f"Error in reducer: {e}")
            raise

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw, reducer=self.reducer)
        ]

if __name__ == '__main__':
    # Set logging level to display all messages
    logging.basicConfig(level=logging.INFO)
    mr_job = MRJaccardSimilarity()
    with mr_job.make_runner() as runner:
        try:
            runner.run()
            results = []
            for key, value in mr_job.parse_output(runner.cat_output()):
                article_pair = key
                similarity = value
                results.append({
                    'Article Pair': list(article_pair),
                    'Jaccard Similarity': similarity
                })
            # Save the results to a JSON file
            with open('jaccard_similarity_results.json', 'w') as json_file:
                json.dump(results, json_file, indent=4)
            logging.info("Results have been written to 'jaccard_similarity_results.json'")
        except Exception as e:
            logging.error(f"Error during job execution: {e}")
            raise

