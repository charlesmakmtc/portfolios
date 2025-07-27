


##################################
######      sample data     ######

def data_init__room_status():
    sql__drop_table = """ DROP TABLE IF EXISTS public.room_status  """

    sql__create_table = """
        CREATE TABLE IF NOT EXISTS public.room_status (
            room_name VARCHAR(10) NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL,
            status VARCHAR(10) NOT NULL
        );
        """
    sql__add_data = """
        INSERT INTO public.room_status (room_name, start_time, end_time, status)
        VALUES
        ('room a', '2023-10-01 09:00:00', '2023-10-01 10:00:00', 'booked'),
        ('room b', '2023-10-01 11:00:00', '2023-10-01 12:00:00', 'available'),
        ('room c', '2023-10-01 14:00:00', '2023-10-01 15:00:00', 'booked'),
        ('room a', '2023-10-02 08:00:00', '2023-10-02 09:00:00', 'available'),
        ('room b', '2023-10-02 10:00:00', '2023-10-02 11:00:00', 'booked'),
        ('room c', '2023-10-02 15:00:00', '2023-10-02 16:00:00', 'available'),
        ('room a', '2023-10-03 10:00:00', '2023-10-03 11:00:00', 'booked'),
        ('room b', '2023-10-03 13:00:00', '2023-10-03 14:00:00', 'available'),
        ('room c', '2023-10-04 11:00:00', '2023-10-04 12:00:00', 'booked'),
        ('room a', '2023-10-04 16:00:00', '2023-10-04 17:00:00', 'available');
    """
    return




############################
######      State     ######

from langgraph.graph.message import AnyMessage
from typing import List, Annotated, Sequence
from typing_extensions import TypedDict
from langgraph.graph import add_messages

class RoomReservationAgentState(TypedDict):
    messages: Annotated[Sequence[AnyMessage], add_messages]


##########################
######      LLM     ######
import os
from langchain_ollama.llms import OllamaLLM, Client
from langchain_ollama import ChatOllama

class llm_ops:
    def get_llm(self) -> ChatOllama:
        llm_server_url    = os.getenv('LLM_SERVER_URL')
        llm_model_name    = "qwen3:14b"
        client = Client(llm_server_url)
        llm = ChatOllama(_client=client, model=llm_model_name, extract_reasoning=False)
        return llm

############################
######      Tools     ######
from datetime import datetime 
from langchain.tools import tool
import psycopg2
from dotenv import load_dotenv
load_dotenv()


        
@tool("tool__check_available_room_timeslots")
def tool__check_available_room_timeslots(targeted_timeslot: datetime):
    """
    Retrieves a list of available room timeslots from the database up to a specified target time.

    This function connects to a PostgreSQL database and queries the `public.room_status` table
    to find all distinct `start_time` entries that are less than or equal to the provided
    `targeted_timeslot`. It is intended to be used as a basic check for available room timeslots,
    though the logic may need further refinement depending on the specific database schema and
    availability rules.

    Args:
        targeted_timeslot (datetime): A datetime object representing the latest time up to
                                      which available timeslots should be considered.

    Returns:
        list: A list of datetime objects representing available room start times.
              Returns an empty list in case of any error during database operations.

    Raises:
        No exceptions are raised explicitly; any errors during execution are caught and
        logged, with an empty list returned as a fallback.

    Notes:
        - This function assumes the existence of a database table named `public.room_status`
          with a column `start_time`.
        - The current query provides a basic check and may not fully represent actual room
          availability (e.g., overlapping bookings are not considered).
        - Database connection parameters are retrieved from environment variables.

    Example:
        >>> from datetime import datetime
        >>> timeslot = datetime(2025, 3, 12, 10, 0)
        >>> available = tool__check_available_room_timeslots(timeslot)
        >>> print("Available timeslots:", available)
        Available timeslots: [datetime.datetime(2025, 3, 12, 9, 0), ...]
    """

    try:
        # Establish a connection to the PostgreSQL database
        db_host = os.getenv('DB_HOST')
        db_port = int(os.getenv('DB_PORT'))
        db_user = os.getenv('DB_USER')
        db_name = "portfolio"
        db_password = os.getenv('DB_PASSWORD')
        
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port
        )
        cur = conn.cursor()

        #   query available with certain timeslot
        query = """
            SELECT DISTINCT start_time
            FROM public.room_status as rs
            WHERE
                rs.start_time <= %s            
        """
        # Execute the query with the targeted_timeslot parameter
        cur.execute(query, (targeted_timeslot,))
        
        # Extract only the room names from the results
        available_timeslots = [row[0] for row in cur.fetchall()]

        return available_timeslots



    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    finally:
        # Ensure the cursor and connection are closed properly
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
    


###########################################################
######      Workflow (include Agents and Prompts)    ######
from langgraph.graph import StateGraph, START, END
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt import create_react_agent
from langgraph_supervisor import create_supervisor


class workflow_ops:
    def __init__(self):
        self.llms = llm_ops()


    def workflow__room_reservation(self, question: str):

        llm = self.llms.get_llm()

        ###     create react agent
        prompt__room_checker = """
            You are a **Room Availability Checker Agent**, designed to help users find available room timeslots up to a specified date and time.

            ### 🎯 Objective:
            When a user asks about room availability, you will:
            1. **Understand the request** — identify the target date and time from the query.
            2. **Use the tool** `tool__check_available_room_timeslots` with the extracted `targeted_timeslot` parameter.
            3. **Return the result** — list the available timeslots in a clear and user-friendly format.
            4. **Handle errors gracefully** — if an error occurs, inform the user and return an empty list.

            ### 📌 Instructions:
            - If the user's query mentions a **specific date and time**, extract it and use it as the `targeted_timeslot`.
            - If the query is **ambiguous** (e.g., "Check availability for next week"), assume a default or ask for clarification (but this agent is not designed to ask questions — use the current date 
            as a fallback).
            - Always format the output as a **list of available timeslots**, using the format:  
            `"Available timeslots: [datetime1, datetime2, ...]"`.
            - If no timeslots are available or an error occurs, return:  
            `"No available timeslots found or an error occurred."`

            ### ✅ Example:
            **User Query:** "Are there any rooms available at 2 PM tomorrow?"
            **Action:** Use the tool with `targeted_timeslot = datetime(2025, 4, 5, 14, 0)`
            **Response:**  
            "Available timeslots: [2025-04-05 10:00:00, 2025-04-05 11:00:00]"

            ### ❗ Error Handling:
            - If the database is unreachable or the query fails, return:  
            `"An error occurred while checking room availability."`

            ### 🔍 Notes:
            - This agent assumes the user's query is clear and provides a valid date/time.
            - You will **not** ask clarifying questions — use the best available information.
            - The tool `tool__check_available_room_timeslots` returns a list of `datetime` objects; you will format these for the user.
            - The tool checks for `start_time <= targeted_timeslot`, which may not fully reflect room availability (e.g., overlapping bookings are not considered).

            ---

            You are now ready to process user queries about room availability.
            """

        room_checker = create_react_agent(
            model=llm,
            tools=[tool__check_available_room_timeslots],
            prompt=prompt__room_checker,
            debug=False,
            name="room_checker"
        )

        ###     create supervisor agent
        prompt__receptionist = """
            You are a **Receptionist Agent**, designed to manage user interactions and coordinate with other agents to provide assistance in a hotel or facility environment.

            ### 🎯 Objective:
            When a user interacts with you, you will:
            1. **Greet the user** and ask how you can assist them.
            2. **Determine the nature of the request** (e.g., room availability, booking, directions, etc.).
            3. **Use the `room_checker` agent only when the request is about checking room availability up to a specific time**.
            4. **Provide a clear and professional response** to the user, using the results from the `room_checker` or other relevant information.
            5. **Handle errors gracefully** — if the `room_checker` fails or returns no results, inform the user appropriately.
            6. **Alternatives recommendation** recommend user any all available timeslots after interact with room_check agent

            ### 📌 Instructions:
            - Use the `room_checker` agent **only** when the user asks about **room availability**.
            - If the user asks for **room booking**, **directions**, or **other services**, respond politely and clarify that you cannot perform those actions but can assist with availability checks.
            - Always format the response in a **friendly and professional tone**, using clear language and appropriate punctuation.
            - If the `room_checker` returns an error or no results, respond with:
            `"I'm unable to check room availability at the moment. Please try again later or contact the front desk for assistance."`
            - If the user's request is **ambiguous**, ask for clarification or proceed with the best available information.

            ### ✅ Example:
            **User Query:** "Do you have any rooms available at 2 PM tomorrow?"
            **Action:** Use the `room_checker` agent with `targeted_timeslot = datetime(2025, 4, 5, 14, 0)`
            **Response:**
            "Available timeslots: [2025-04-05 10:00:00, 2025-04-05 11:00:00]"

            **User Query:** "I want to book a room for tomorrow."
            **Response:**
            "I currently cannot assist with room bookings. However, I can help you check the availability of rooms. Would you like me to check for availability at a specific time?"

            **User Query:** "Where is the nearest restrooms?"
            **Response:**
            "I’m unable to provide directions or locate facilities within the building. Please ask a staff member for assistance."

            ### ❗ Error Handling:
            - If the `room_checker` agent returns an error or no results, respond with:
            `"I'm unable to check room availability at the moment. Please try again later or contact the front desk for assistance."`
            - If the user's request is unclear or outside the scope of your capabilities, politely inform them and offer to help with availability checks.

            ### 🔍 Notes:
            - This agent is **not designed to perform bookings** or other administrative tasks — it only coordinates with the `room_checker` agent.
            - You will **not ask questions** unless the user's request is ambiguous — use the best available information to proceed.
            - The `room_checker` agent returns a list of `datetime` objects; you will format these for the user in a clear and user-friendly way.
            - Always keep the tone **professional and helpful**, even in cases of errors or limitations.

            ---

            You are now ready to interact with users and manage the `room_checker` agent to provide assistance with room availability.
            """

        receptionist = create_supervisor(
            [room_checker],
            model=llm,
            prompt=prompt__receptionist,
            supervisor_name='receptionist'
        )

        app = receptionist.compile()


        ###     human question
        print(f"[workflow__room_reservation] human_question: {question}")

        response = app.invoke({"messages": [{"role": "user", "content": question}]})

        return response

##############################
######      chatbot     ######

def main():
    human_question = "I would like to book a room on 2023-10-02 after 1pm, can you check any availabile timeslots for me?"
    w = workflow_ops()
    ai_answers = w.workflow__room_reservation(question=human_question)
    last_msg = ai_answers['messages'][-1].content
    print("\n\nlast_msg as below")
    print(last_msg)
    return


if __name__ == "__main__":
    main()
    