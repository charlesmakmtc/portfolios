

from langchain_ollama import ChatOllama
from langchain_ollama import OllamaEmbeddings
from langchain_ollama.llms import Client
from langchain_core.vectorstores import InMemoryVectorStore
from langchain.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
import os
from dotenv import load_dotenv
load_dotenv()




def smartwatch_product_recommendation_chatbot(human_question=""):
    #   setup LLM server
    llm_server_url    = os.getenv('LLM_SERVER_URL')
    llm_model_name    = "qwen3:14b"
    client = Client(llm_server_url)
    llm = ChatOllama(_client=client, model=llm_model_name)
    embeddings = OllamaEmbeddings(model=llm_model_name)

    #   smartwatch product catalog 
    context = """
    Below are our smart watch products, including product features and introduction::
        - Product Code: "ABC123"
            - product grade: military
            - product features: 
                - waterproof (IP68)
                - long battery life (support over 20 days)
                - weight is heavy (300g)
                
        - Product Code: "DEF456
            - product grade: leisure
            - product features: 
                - weight is light (50g)
                - non-waterproof
                - medium battery life (~ 50 hours)
    """

    #   RAG (convert smartwatch product catalog to vectorstore)
    vectorstore = InMemoryVectorStore.from_texts([context], embedding=embeddings)
    retriever = vectorstore.as_retriever()

    #   write single agent & prompt
    system_message = """
    Answer the following question based on this context but Do not mention the information is provided by context:
    Question: {human_question}

    Based on the above context:
    When you recommend a product based on the `product_catalog_context`, you must think step by step.
    """

    product_catalog = """
        Below are product catalog: 
            <product_catalog_context>
            {product_catalog_context}
            </product_catalog_context>
    """

    #   setup chain
    chain = ChatPromptTemplate.from_messages([
        ("system", system_message),
        ("system", product_catalog),
        ("human", "{human_question}")
    ])

    Product_Recommendation_QA_chain = (
            {
                "product_catalog_context": retriever,
                "human_question": RunnablePassthrough(),
            } | chain | llm | StrOutputParser()
    )

    #   chatbot session start
    ai_product_recommendation = Product_Recommendation_QA_chain.invoke(input=human_question)
    print(ai_product_recommendation)
    print("[2025-07-26 11:00] ai_product_recommendation")
    return



def main():
    human_question = "Recommend me a product for me. I am a business man, and love outdoor activities under water. List out all for me"
    smartwatch_product_recommendation_chatbot(human_question=human_question)
    return

if __name__ == "__main__":
    main()