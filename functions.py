import re
import datetime

from database import Database

MENU_START: int = 0
MENU_LOGIN: int = 1
MENU_REGISTER: int = 2
MENU_ADMIN: int = 3
MENU_USER: int = 4
MENU_QUESTION: int = 1000

db = Database()
user_id: int | None = None
status: bool = True

def initialize_game():
    # Connect or Create Database
    db.connect()

    # Display Menu until game status is disabled
    while status:
        display_menu(MENU_START)

def display_menu(menu_id):
    global user_id

    # ========================================= [START MENU] ============================================

    # MENU_START
    if menu_id == MENU_START:
        while True:
            selection = input("\n" * 100 + "Please select an option from the menu:\n1. Login\n2. Register\nInsert your choice: ")
            if not selection.isnumeric():
                input("Invalid input! Press ENTER to continue...")
                continue
            selection = int(selection)

            # MENU_START -> Login (1)
            if selection == 1:
                username: str = input("\n" * 100 + "Please enter your credentials!\nUsername: ")
                password: str = input("Password: ")
                if username == 'admin' and password == 'admin':
                    display_menu(MENU_ADMIN)
                else:
                    correct_password: bool = db.is_password_matching(username, password)
                    # While password is invalid
                    while not correct_password and password != 'EXIT':
                        password = input("\n" * 100 + "Invalid password, please try again!\nTo exit please type 'EXIT'.\nPassword: ")
                        correct_password = db.is_password_matching(username, password)
                    # If successfully logged in
                    if correct_password:
                        user_id = db.get_user_id(username)
                        display_menu(MENU_USER)

            # MENU_START -> Register (2)
            elif selection == 2:

                # Register -> Username
                username: str = input("\n" * 100 + "Please enter your desired username!\nUsername: ")
                while db.get_user_id(username) is not None:
                    username = input("\n" * 100 + "This username is already taken, please pick something else!\nUsername: ")

                # Register -> Password
                # Password regex pattern
                password_pattern = re.compile(r"^(?=.*[A-Za-z])(?=.*\d)(?=.*[^A-Za-z0-9]).{6,32}$")
                password: str = input("\n" * 100 + "Please enter your desired password!\nPassword Rules: 6-32 characters, must include alphabetic characters, numbers, and a special symbol!\nPassword: ")
                while not password_pattern.match(password):
                    password: str = input("\n" * 100 + "Your password does not meet the criteria!\nPassword Rules: 6-32 characters, must include alphabetic characters, numbers, and a special symbol!\nPassword: ")

                # Register -> Email
                email: str = input("\n" * 100 + "Please enter your e-mail address:\nE-mail: ")
                email_pattern = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
                while not email_pattern.match(email):
                    email = input("\n" * 100 + "Invalid email address!:\nE-mail: ")

                # Register -> D.O.B
                dob: str = input("\n" * 100 + "Please enter your date-of-birth (YYYY-MM-DD)!\nD.O.B: ")
                dob_pattern = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
                while not dob_pattern.match(dob):
                    dob = input("\n" * 100 + "Invalid date-of-birth, please use YYYY-MM-DD format!\nD.O.B: ")
                dob_date = datetime.datetime.strptime(dob, "%Y-%m-%d").date()

                # Create user & Move to Player Menu
                user_id = db.create_user(username, password, email, dob_date)
                display_menu(MENU_USER)
            else:
                input("Invalid input! Press ENTER to continue...")
                continue


    # ========================================= [ADMINISTRATIVE MENU] ============================================

    # Administrative Menu
    if menu_id == MENU_ADMIN:
        while True:
            selection = input("\n" * 100 + "Please select an option from the menu:\n1. Create a Question\n2. View Game Statistics\n3. Log out\nInsert your choice: ")
            if not selection.isnumeric():
                input("Invalid input! please press ENTER and try again...")
                continue
            selection = int(selection)

            # Admin Menu -> Create Database
            if selection == 1:
                question: str = input("\n" * 100 + "Please enter the body of the question!\nQuestion: ")
                answer_1: str = input("\n" * 100 + "Please enter the first answer!\nAnswer 1: ")
                answer_2: str = input("\n" * 100 + "Please enter the second answer!\nAnswer 2: ")
                answer_3: str = input("\n" * 100 + "Please enter the third answer!\nAnswer 3: ")
                answer_4: str = input("\n" * 100 + "Please enter the fourth answer!\nAnswer 4: ")
                correct_answer: int = int(input("\n" * 100 + "Please enter the number of the correct answer!\nCorrect Answer: "))
                confirmation: str = input("\n" * 100 + f"Question: {question}\nAnswer 1: {answer_1}\nAnswer 2: {answer_2}\nAnswer 3: {answer_3}\nAnswer 4: {answer_4}\nCorrect Answer: {correct_answer}\nAre you sure you would like to create this question? Y/N: ")
                if confirmation.lower() == 'y':
                    question_id: int | None = db.create_question(question, answer_1, answer_2, answer_3, answer_4, correct_answer)
                    if question_id is not None:
                        print(f"Question #{question_id} has been successfully created!")
                    else:
                        print(f"Something went wrong, question_id returned None upon creation!")
                else:
                    input("Question creation process has been stopped! Press ENTER to return...")
                    continue


            # Admin Menu -> View Game Statistics
            elif selection == 2:
                while True:
                    selection = input("\n" * 100 +
                        "Please select which of the following statistics would you like to display:\n" +
                        "1. Count of users who have played\n" +
                        "2. Easiest Question(s)\n" +
                        "3. Hardest Question(s)\n" +
                        "4. Users who have answered the most questions correctly\n" +
                        "5. Users who have answered the least questions\n" +
                        "6. View user answers\n" +
                        "7. Question-specific statistics\n" +
                        "8. Return\n" +
                        "Insert your choice: ")
                    if not selection.isnumeric():
                        input("Invalid input! please press ENTER and try again...")
                        continue
                    selection = int(selection)

                    # Count of users who have played
                    if selection == 1:
                        result = db.query("SELECT COUNT(*) FROM users WHERE questions_solved <> 0", 1)
                        input("\n" * 100 + f"Users who have played: {result[0]}\nPress ENTER to return...")

                    # Easiest Question(s)
                    elif selection == 2:
                        result = db.query("""
                            SELECT q.question, COUNT(*) as solved_correctly
                            FROM user_answers ua
                            JOIN questions q ON ua.question_id = q.id
                            WHERE ua.is_correct = TRUE
                            GROUP BY q.question
                            HAVING COUNT(*) = (
                                SELECT MAX(cnt)
                                FROM (
                                    SELECT COUNT(*) cnt
                                    FROM user_answers
                                    WHERE is_correct = TRUE
                                    GROUP BY question_id
                                )
                            )
                        """, 2)
                        print("\n" * 100)
                        for row in result:
                            print(f"{row[0]} ({row[1]} solved correctly)")
                        input("Press ENTER to return...")

                    # Easiest Question(s)
                    elif selection == 3:
                        result = db.query("""
                            SELECT q.question, COUNT(*) as solved_correctly
                            FROM user_answers ua
                            JOIN questions q ON ua.question_id = q.id
                            WHERE ua.is_correct = TRUE
                            GROUP BY q.question
                            HAVING COUNT(*) = (
                                SELECT MIN(cnt)
                                FROM (
                                    SELECT COUNT(*) cnt
                                    FROM user_answers
                                    WHERE is_correct = TRUE
                                    GROUP BY question_id
                                )
                            )
                        """, 2)
                        print("\n" * 100)
                        for row in result:
                            print(f"{row[0]} ({row[1]} solved correctly)")
                        input("Press ENTER to return...")

                    # Users who have answered the most questions correctly
                    elif selection == 4:
                        result = db.query("""
                            SELECT u.username, COUNT(*) solved_correctly
                            FROM user_answers ua
                            JOIN users u ON ua.user_id = u.id
                            WHERE ua.is_correct = TRUE
                            GROUP BY u.username
                            ORDER BY solved_correctly DESC
                            LIMIT 100 -- For safety
                        """, 2)
                        print("\n" * 100)
                        for row in result:
                            print(f"{row[0]} ({row[1]} questions solved correctly)")
                        input("Press ENTER to return...")

                    # Users who have answered the most questions (regardless whether they're correct or not)
                    elif selection == 5:
                        result = db.query("""
                            SELECT u.username, COUNT(*) solved
                            FROM user_answers ua
                            JOIN users u ON ua.user_id = u.id
                            GROUP BY u.username
                            ORDER BY solved DESC
                            LIMIT 100 -- For safety
                        """, 2)
                        print("\n" * 100)
                        for row in result:
                            print(f"{row[0]} ({row[1]} questions solved)")
                        input("Press ENTER to return...")

                    # View user answers
                    elif selection == 6:
                        while True:
                            target_id = input("\n" * 100 + "Please enter the User ID of the targeted user:\nUser ID: ")
                            if target_id.isnumeric():
                                target_id = int(target_id)
                                break
                        user_answers = db.get_user_answers(target_id)
                        for row in user_answers:
                            print(f"{row[0]} ({row[1]})")
                        input("Press ENTER to return...")

                    # Question Statistics (Bonus)
                    elif selection == 7:
                        result = db.query("""
                            SELECT 
                              q.question,
                              COUNT(ua.*) AS total_answers,
                              COUNT(CASE WHEN ua.is_correct = TRUE THEN 1 END) AS correct_answers,
                              COUNT(CASE WHEN ua.is_correct = FALSE THEN 1 END) AS incorrect_answers
                            FROM questions q
                            LEFT JOIN user_answers ua ON ua.question_id = q.id
                            GROUP BY q.id, q.question
                        """, 2)
                        print("\n" * 100)
                        for row in result:
                            print(f"{row[0]} [Answers: {row[1]}] [Correct Answers: {row[2]}] [Incorrect Answers: {row[3]}]")
                        input("Press ENTER to return...")

                    # Return
                    elif selection == 8:
                        display_menu(MENU_ADMIN)
                        return

            # Admin Menu -> Log out
            elif selection == 3:
                user_id = None
                display_menu(MENU_START)
                return



    # ========================================= [USER MENU] ============================================

    # Players (non-admin) menu
    elif menu_id == MENU_USER:
        while True:
            selection = input("\n" * 100 + "Please select an option from the menu:\n1. Play\n2. My Statistics\n3. Hall of Fame\n4. Log out\nInsert your choice: ")
            if not selection.isnumeric():
                input("Invalid input! Press ENTER to continue...")
                continue
            selection = int(selection)

            # Player Menu -> Play
            if selection == 1:

                # If user stopped mid-game last time
                questions_solved = db.get_user_questions_solved(user_id)
                while questions_solved != 0:
                    selection = input(
                        "\n" * 100 + f"Would you like to continue the game from where you left?\n1. Continue\n2. Start Over\nInsert your choice: ")
                    if not selection.isnumeric():
                        input("Invalid input! Press ENTER to continue...")
                        continue
                    selection = int(selection)
                    if selection == 1:
                        break
                    elif selection == 2:
                        db.reset_user_answers(user_id)
                        break

                play()


            # Player Menu -> My Statistics
            elif selection == 2:
                us = db.get_user_statistics(user_id)
                input("\n" * 100 + f"Unique ID: {us[0]}\nUsername: {us[1]}\nE-mail: {us[3]}\nD.O.B: {us[4]}\nQuestions Solved: {us[5]} ({us[7]}/{us[5]} are correct)\nLast Played: {us[6]}\nPress ENTER to return...")
                display_menu(MENU_USER)
                return

            # Hall of Fame
            elif selection == 3:
                result = db.query("""
                    SELECT u.username, COUNT(*) correct_answers, to_char(MAX(ua.answer_timestamp) - u.play_timestamp, 'HH24:MI:SS.MS') AS playtime
                    FROM user_answers ua
                    JOIN users u ON ua.user_id = u.id
                    WHERE ua.is_correct = TRUE
                    GROUP BY u.username, u.play_timestamp
                    ORDER BY correct_answers DESC, playtime ASC
                """, 2)
                print("\n" * 100)
                if result is None:
                    input("Looks like no player has played yet...\nPress ENTER to return...")
                else:
                    for row in result:
                        print(f"{row[0]}: {row[1]} correct answers (Time: {row[2]})")
                    input("Press ENTER to return...")

            # Player Menu -> Log out
            elif selection == 4:
                user_id = None
                display_menu(MENU_START)
                return

def play():
    # If player play timestamp is NULL -> Set it to current timestamp
    play_timestamp: datetime.datetime | None = db.get_user_play_timestamp(user_id)
    if play_timestamp is None:
        db.update_user_play_timestamp(user_id)

    question_data: tuple[int, str, str, str, str, str, int] | None = db.get_user_question(user_id)
    if question_data[0] is None:
        reset = input("\n" * 100 + "Congratulations, you've answered all of the questions!\nIf you're interested to start all over again type 'RESET', otherwise press ENTER... ")
        if reset.lower() == 'reset':
            db.reset_user_answers(user_id)
        display_menu(MENU_USER)
        return

    while True:
        selection = input("\n" * 100 + f"{question_data[1]}\n1. {question_data[2]}\n2. {question_data[3]}\n3. {question_data[4]}\n4. {question_data[5]}\n5. Exit\nInsert your answer: ")
        if not selection.isnumeric():
            input("Invalid input! Press ENTER to continue...")
            continue
        selection = int(selection)

        # Exit
        if selection == 5:
            display_menu(MENU_USER)
            return
        if selection in (1, 2, 3, 4):
            db.handle_user_answer(user_id, question_data[0], selection)
            input(f"You are {'correct' if selection == question_data[6] else 'wrong'}! The right answer is #{question_data[6]}.")
            play()
            return