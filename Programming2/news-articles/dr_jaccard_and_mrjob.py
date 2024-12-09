import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# Regular expression to tokenize words
WORD_RE = re.compile(r"[\w']+")

class MRJaccardSimilarity(MRJob):

    def mapper_raw(self, input_path, input_uri):
        # Read the entire input file
        with open(input_path, 'r') as f:
            json_data = f.read()
        # Parse the JSON array
        articles = json.loads(json_data)
        for article in articles:
            content = article.get("Content", "").lower()
            article_id = article.get("Title", "unknown")
            # Tokenize and yield words
            for word in WORD_RE.findall(content):
                yield word, article_id

    def reducer(self, word, article_ids):
        # Determine if the word is in both articles
        article_ids = set(article_ids)
        if len(article_ids) == 2:
            yield 'Counts', ('Intersection', 1)
        yield 'Counts', ('Union', 1)

    def reducer_counts(self, key, values):
        # Accumulate counts of intersection and union
        intersection_count = 0
        union_count = 0
        for count_type, count in values:
            if count_type == 'Intersection':
                intersection_count += count
            elif count_type == 'Union':
                union_count += count
        # Emit total counts to the final reducer
        yield 'Jaccard', (intersection_count, union_count)

    def reducer_final(self, key, values):
        # Calculate Jaccard similarity
        total_intersection = 0
        total_union = 0
        for intersection_count, union_count in values:
            total_intersection += intersection_count
            total_union += union_count
        if total_union > 0:
            jaccard_similarity = total_intersection / total_union
        else:
            jaccard_similarity = 0
        yield 'Jaccard Similarity', jaccard_similarity

    def steps(self):
        return [
            MRStep(mapper_raw=self.mapper_raw, reducer=self.reducer),
            MRStep(reducer=self.reducer_counts),
            MRStep(reducer=self.reducer_final)
        ]

if __name__ == '__main__':
    MRJaccardSimilarity.run()

