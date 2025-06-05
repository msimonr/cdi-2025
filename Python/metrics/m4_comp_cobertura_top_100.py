# ---
# MET5AP1_CompCobertura_Top100
# Verifica si los libros del top 100 están presentes en la base (por título + autor)
# ---

def met5ap1_comp_cobertura_top100(conn_db, conn_dbdq, table_libros, table_authors, id_exec):
    
    TIME_100_BOOKS = [
    ("The Adventures of Augie March", "Saul Bellow"),
    ("All the King's Men", "Robert Penn Warren"),
    ("American Pastoral", "Philip Roth"),
    ("An American Tragedy", "Theodore Dreiser"),
    ("Animal Farm", "George Orwell"),
    ("Appointment in Samarra", "John O’Hara"),
    ("Are You There God? It’s Me, Margaret", "Judy Blume"),
    ("The Assistant", "Bernard Malamud"),
    ("At Swim-Two-Birds", "Flann O’Brien"),
    ("Atonement", "Ian McEwan"),
    ("Beloved", "Toni Morrison"),
    ("The Berlin Stories", "Christopher Isherwood"),
    ("The Big Sleep", "Raymond Chandler"),
    ("The Blind Assassin", "Margaret Atwood"),
    ("Blood Meridian", "Cormac McCarthy"),
    ("Brideshead Revisited", "Evelyn Waugh"),
    ("The Bridge of San Luis Rey", "Thornton Wilder"),
    ("Call It Sleep", "Henry Roth"),
    ("Catch-22", "Joseph Heller"),
    ("The Catcher in the Rye", "J.D. Salinger"),
    ("A Clockwork Orange", "Anthony Burgess"),
    ("The Confessions of Nat Turner", "William Styron"),
    ("The Corrections", "Jonathan Franzen"),
    ("The Crying of Lot 49", "Thomas Pynchon"),
    ("A Dance to the Music of Time", "Anthony Powell"),
    ("The Day of the Locust", "Nathanael West"),
    ("Death Comes for the Archbishop", "Willa Cather"),
    ("A Death in the Family", "James Agee"),
    ("The Death of the Heart", "Elizabeth Bowen"),
    ("Deliverance", "James Dickey"),
    ("Dog Soldiers", "Robert Stone"),
    ("Falconer", "John Cheever"),
    ("The French Lieutenant’s Woman", "John Fowles"),
    ("The Golden Notebook", "Doris Lessing"),
    ("Go Tell It on the Mountain", "James Baldwin"),
    ("Gone with the Wind", "Margaret Mitchell"),
    ("The Grapes of Wrath", "John Steinbeck"),
    ("Gravity’s Rainbow", "Thomas Pynchon"),
    ("The Great Gatsby", "F. Scott Fitzgerald"),
    ("A Handful of Dust", "Evelyn Waugh"),
    ("The Heart Is a Lonely Hunter", "Carson McCullers"),
    ("The Heart of the Matter", "Graham Greene"),
    ("Herzog", "Saul Bellow"),
    ("Housekeeping", "Marilynne Robinson"),
    ("A House for Mr. Biswas", "V.S. Naipaul"),
    ("I, Claudius", "Robert Graves"),
    ("Infinite Jest", "David Foster Wallace"),
    ("Invisible Man", "Ralph Ellison"),
    ("Light in August", "William Faulkner"),
    ("The Lion, the Witch and the Wardrobe", "C.S. Lewis"),
    ("Lolita", "Vladimir Nabokov"),
    ("Lord of the Flies", "William Golding"),
    ("The Lord of the Rings", "J.R.R. Tolkien"),
    ("Loving", "Henry Green"),
    ("Lucky Jim", "Kingsley Amis"),
    ("The Man Who Loved Children", "Christina Stead"),
    ("Midnight’s Children", "Salman Rushdie"),
    ("Money", "Martin Amis"),
    ("The Moviegoer", "Walker Percy"),
    ("Mrs. Dalloway", "Virginia Woolf"),
    ("Naked Lunch", "William Burroughs"),
    ("Native Son", "Richard Wright"),
    ("Neuromancer", "William Gibson"),
    ("Never Let Me Go", "Kazuo Ishiguro"),
    ("1984", "George Orwell"),
    ("On the Road", "Jack Kerouac"),
    ("One Flew Over the Cuckoo’s Nest", "Ken Kesey"),
    ("The Painted Bird", "Jerzy Kosinski"),
    ("Pale Fire", "Vladimir Nabokov"),
    ("A Passage to India", "E.M. Forster"),
    ("Play It as It Lays", "Joan Didion"),
    ("Portnoy’s Complaint", "Philip Roth"),
    ("Possession", "A.S. Byatt"),
    ("The Power and the Glory", "Graham Greene"),
    ("The Prime of Miss Jean Brodie", "Muriel Spark"),
    ("Rabbit, Run", "John Updike"),
    ("Ragtime", "E.L. Doctorow"),
    ("The Recognitions", "William Gaddis"),
    ("Red Harvest", "Dashiell Hammett"),
    ("Revolutionary Road", "Richard Yates"),
    ("The Sheltering Sky", "Paul Bowles"),
    ("Slaughterhouse-Five", "Kurt Vonnegut"),
    ("Snow Crash", "Neal Stephenson"),
    ("The Sot-Weed Factor", "John Barth"),
    ("The Sound and the Fury", "William Faulkner"),
    ("The Sportswriter", "Richard Ford"),
    ("The Spy Who Came In from the Cold", "John le Carré"),
    ("The Sun Also Rises", "Ernest Hemingway"),
    ("Their Eyes Were Watching God", "Zora Neale Hurston"),
    ("Things Fall Apart", "Chinua Achebe"),
    ("To Kill a Mockingbird", "Harper Lee"),
    ("To the Lighthouse", "Virginia Woolf"),
    ("Tropic of Cancer", "Henry Miller"),
    ("Ubik", "Philip K. Dick"),
    ("Under the Net", "Iris Murdoch"),
    ("Under the Volcano", "Malcolm Lowry"),
    ("Watchmen", "Alan Moore & Dave Gibbons"),
    ("White Noise", "Don DeLillo"),
    ("White Teeth", "Zadie Smith"),
    ("Wide Sargasso Sea", "Jean Rhys")
]
    
    with conn_db.cursor() as cur:
        cantidad_encontrados = 0
        for title, author in TIME_100_BOOKS:

            cur.execute(f'''
                SELECT L.id
                FROM "{table_libros}" L
                JOIN "{table_authors}" A ON L.author_id = A.idauthor
                WHERE LOWER(L.title) = LOWER(%s)
                AND LOWER(A.author) = LOWER(%s)
            ''', (title, author))
            
            if cur.fetchone():
                cantidad_encontrados += 1 

        with conn_dbdq.cursor() as cur2:
            cur2.execute(
                '''INSERT INTO "ResultTable" 
                (target_table, id_exec, id_ap_method, dq_value)
                VALUES (%s, %s, %s, %s)''',
                (table_libros, id_exec, 'MET5AP1_CompCobertura_Top100', cantidad_encontrados/100)
            )
