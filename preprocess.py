import spacy
import en_core_web_sm


def pre_process(abstract: str):
    # Load the English language model in spaCy
    nlp = en_core_web_sm.load()

    # Define the abstract text
    abstract = abstract

    # Remove unwanted characters and normalize the text
    abstract = abstract.replace('\n', ' ').replace('\r', '').strip()

    # Tokenize the abstract into sentences and words using spaCy
    doc = nlp(abstract)
    # sentences = [sent.string.strip() for sent in doc.sents]
    words = [token.text for token in doc if not token.is_punct and not token.is_stop]
    res = ' '.join(words)
    return res
    # # Write the cleaned text to a file
    # with open('cleaned_abstract.txt', 'w') as f:
    #     f.write(' '.join(words))