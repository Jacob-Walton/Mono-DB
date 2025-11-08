make table users
    as relational
    fields
        id int primary key unique
        first_name text
        last_name text
        email text unique
        created_at date default now()
        status text default "inactive"


put into users
    first_name = "Jacob"
    last_name = "Walton"
    email = "jacob@example.com"
    created_at = now()
    status = "inactive"
