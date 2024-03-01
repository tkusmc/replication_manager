import socket
import database


# Known servers
servers = [
    {
        "prod1": "192.168.1.122",
        "prod2": "192.168.1.123"
    },
    {
        "prod_m1": "192.168.1.122", # prod mirror
        "prod_m2": "192.168.1.122"
    },
    {
        "int1": "172.16.50.122",
        "int2": "172.16.50.122"
    },
    {
        "dev1": "192.168.1.122",
        "dev2": "192.168.1.122"
    },
]


def main():
    choice = 0
    working = 'Working...'

    local_ip      = '192.168.1.122'
    remote_ip     = '192.168.1.123'

    print(menu())

    while not choice == 'q':
        choice = input('Choose: ')

        if choice == '1':
            rep_status()

        elif choice == '2':
            # rebuilds subscriptions on BOTH servers
            rebuild_subscriptions()

        elif choice == '3':
            create_tables()

        elif choice == '4':
            # rebuilds replication on BOTH servers by:
            #   dropping subscriptions
            #   removing replication
            #   initializing replication
            #   creating subscriptions
            rebuild_replication()

        elif choice == '5':
            # disables subscription on local server ONLY
            disable_subscription()

        elif choice == '6':
            # enables subscription on local server ONLY
            enable_subscription()

        elif choice == '7':
            insert_data()

        elif choice == '8':
            display_message(working)
            run_e164()

        elif choice == '9':
            run_update()

        elif choice == '10':
            lip=(get_local_ip())
            print(lip)

            # TEST DATA
            lip = '172.16.34.121'

            print(get_remote_ip(lip))

            local_ip      = get_local_ip()
            if local_ip:
                remote_ip       = get_remote_ip(local_ip)


def menu():
    menu = '- Simplicity Replication Validation and Repair Assistant -\n\n'
    menu += 'Please select from the following list:\n'
    menu += ' 1.  Get Replication Status\n'
    menu += ' 2.  Rebuild Subscriptions\n'
    menu += ' 3.  Check Replication\n'
    menu += ' 4.  Rebuild Replication\n'
    menu += ' 5.  Disable Subscription\n'
    menu += ' 6.  Enable Subscription\n'
    menu += ' 7.  Create/Update Initial Data and Settings\n'
    menu += ' 8.  Create/Update e164 Data\n'
    menu += ' 9.  Update Database (Items 3, 4, 6, 7)\n'
    menu += '10.  Run All (Items 1 - 8)\n'
    menu += 'q to Quit\n'

    return menu


def display_message(msg):
    print(msg)


# function to call all the other functions.
def rep_status():

    print(get_status())


# REPLICATION MENU FUNCTIONS
def get_status():
    # Gets the replication status

    # get db connection
    db_local = database.get_db(local_ip)

    for i in db_local:
        data = i.execute("""SELECT * FROM replication_check_status() """).fetchall()

        return data


def rebuild_subscriptions():
    # Drop/Create local and remote subscriptions

    # drop subscriptions on primary and secondary servers
    drop_subscriptions()

    # create subscriptions on primary and secondary servers
    create_subscriptions()

    # TODO return status from both servers


def check_replication():

    try:
        pass
    except Exception as err:
        pass
    else:
        pass


def rebuild_replication():
    # Removes replication subs, pubs, & init and rebuilds it.
    # This is handy if you're not sure what's wrong.

    # drop subscriptions on primary and secondary servers
    drop_subscriptions()

    # remove replication
    remove_replication()

    # initialize on both servers
    initialize_replication()   # TODO ADD PARAMETERS

    # create subscriptions on primary and secondary servers
    create_subscriptions()


def disable_subscription(local_ip: str):
    # get db connection
    db_local = database.get_db(local_ip)

    for i in db_local:
        data = i.execute("""SELECT * FROM replication_disable_simplicity_subscriptions() """).fetchall()

        return data


def enable_subscription(local_ip: str):
    # get db connection
    db_local = database.get_db(local_ip)

    # try:
    for i in db_local:
        data = i.execute("""SELECT * FROM replication_enable_simplicity_subscriptions() """).fetchall()

        return data


# HELPER FUNCTIONS
def drop_subscriptions(local_ip: str, remote_ip: str):
    # get db connection
    db_local = database.get_db(local_ip)
    db_remote = database.get_db(remote_ip)

    # try:
    for i in db_local:
        data = i.execute("""SELECT * FROM replication_drop_simplicity_subscriptions() """).fetchall()

    for i in db_remote:
        data = i.execute("""SELECT * FROM replication_drop_simplicity_subscriptions() """).fetchall()

    return data


def create_subscriptions():
    # get db connection
    db_local = database.get_db(primary_ip)
    db_remote = database.get_db(secondary_ip)

    # try:
    for i in db_local:
        data = i.execute("""SELECT * FROM replication_create_simplicity_subscriptions() """).fetchall()

    for i in db_remote:
        data = i.execute("""SELECT * FROM replication_create_simplicity_subscriptions() """).fetchall()

    return data


def remove_replication():
    # Make sure subscriptions are dropped first.

    # TODO Ensure there are no subscriptions first

    # get db connection
    db_local = database.get_db(primary_ip)
    db_remote = database.get_db(secondary_ip)

    # remove replication
    for i in db_local:
        data = i.execute("""SELECT * FROM replication_remove() """).fetchall()

    # remove replication
    for i in db_remote:
        data = i.execute("""SELECT * FROM replication_remove() """).fetchall()


def initialize_replication(in_primary_ip: str, in_secondary_ip: str):
    # TODO figure out input parameters and check system_locals

    # get db connection
    db_local = database.get_db(primary_ip)
    db_remote = database.get_db(secondary_ip)

    # remove replication
    for i in db_local:
        data = i.execute("""SELECT * FROM replication_config_init(%,%) """,
                         (in_primary_ip, in_secondary_ip)).fetchall()

    # remove replication
    for i in db_remote:
        data = i.execute("""SELECT * FROM replication_config_init() """,
                         (in_primary_ip, in_secondary_ip)).fetchall()


def get_local_ip():
    # get the local ip address
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Connect to machine accessible to all
    s.connect(("172.16.51.253", 80))
    lip = s.getsockname()[0]
    s.close()

    return lip


def get_remote_ip(local_ip):
    # get the remote ip address
    # NOTE - THIS IS NOT THE SECONDARY IP, IT'S SIMPLY THE OTHER SERVER'S IP.
    print(local_ip)
    # get db connection
    db = database.get_db(local_ip)

    for i in db:
        # TODO MAKE A SQL FUNCTION INSTEAD OF THE ADHOC QUERY HERE.
        # TODO MAKE FUNCTION GET OTHER IP BUT ALSO MAKE SURE IT CONTAINS THE LOCAL_IP
        data = i.execute("""SELECT host FROM system_replication sr WHERE sr.host != (%s) """,
                         (local_ip,)).fetchall()
        data = res = [ sub['host'] for sub in data ]

    return data[0]



if __name__ == "__main__":
    main()
