import socket
import time
import database


def main():
    choice = 0
    working = 'Working...'

    ip = {}

    # initialize ip dict

    # get the local ip. NOTE - this is the ip of the server where you're running this program, so it's relative.
    ip["local_ip"] = get_local_ip()

    # make sure local ip is not null
    if ip.get("local_ip"):
        # get the system replication table from the local_ip db
        res = get_system_replication( ip["local_ip"] )

        contains_local = None
        for d in res:
            name = d.get("name")
            host = d.get("host")
            # populate ip["primary'] and ip["secondary]
            ip[name] = host

            if ip.get("local_ip") != host:
                # the remote ip is the ip that is not the local ip
                # NOTE - this is the server that is REMOTE to the server where you're running this, so it's relative
                ip["remote_ip"] = host
            else:
                # system_replication must contain the local ip address as on of its entries
                # tracking this with contains_local
                contains_local = True

            if contains_local == False:
                exit(1)


    # print(ip.get("local_ip"))
    # print(ip.get("remote_ip"))
    # print(ip.get("primary"))
    # print(ip.get("secondary"))

    # print the main menu
    print(menu(ip.get("local_ip"), ip.get("remote_ip")))

    while not choice == 'q':
        choice = input('Choose: ')

        if choice == '1':
            # get the full replication status

            res = get_status(ip.get("local_ip"))

            for d in res:
                print(d)
            print()
            res = get_status(ip.get("remote_ip"))
            for d in res:
                print(d)

        elif choice == '2':
            # rebuilds subscriptions on BOTH servers
            rebuild_subscriptions(ip.get("local_ip"), ip.get("remote_ip"))

            # get sub status after rebuild
            print(get_sub_status(ip.get("local_ip")))
            print()
            print(get_sub_status(ip.get("remote_ip")))

        elif choice == '3':
            # rebuilds replication on BOTH servers by:
            #   dropping subscriptions
            #   removing replication
            #   initializing replication
            #   creating subscriptions
            rebuild_replication(ip.get("local_ip"), ip.get("remote_ip"), ip.get("primary"), ip.get("secondary"))

            # get sub status after rebuild
            print(get_sub_status(ip.get("local_ip")))
            print()
            print(get_sub_status(ip.get("remote_ip")))

        elif choice == '4':
            # disables subscription on local server ONLY
            disable_subscription(ip.get("local_ip"))

            # get sub status
            res = get_sub_status(ip.get("local_ip"))

            for d in res:
                print(f'{d["item"]}: {d["value"]}')
            print()

        elif choice == '5':
            # enables subscription on local server ONLY
            enable_subscription(ip.get("local_ip"))

            # get sub status
            res = get_sub_status(ip.get("local_ip"))

            for d in res:
                print(f'{d["item"]}: {d["value"]}')
            print()

        elif choice == '6':
            diagnostic(ip.get("local_ip"), ip.get("remote_ip"), 1, ip.get("primary"), ip.get("secondary"))

        elif choice == '7':
            diagnostic(ip.get("local_ip"), ip.get("remote_ip"), 2, ip.get("primary"), ip.get("secondary"))

        elif choice == 'secret':
            pass

        else:
            print(menu(ip.get("local_ip"), ip.get("remote_ip")))


def menu(local, remote):
    menu = '- Simplicity Replication Validation and Repair Assistant -\n\n'
    menu += 'Please select from the following list:\n'
    menu += f' 1.  Get Replication Status: LOCAL [{local}] | REMOTE [{remote}]\n'
    menu += f' 2.  Rebuild Subscriptions:  LOCAL [{local}] | REMOTE [{remote}]\n'
    menu += f' 3.  Rebuild Replication:    LOCAL [{local}] | REMOTE [{remote}]\n'
    menu += f' 4.  Disable Subscription:   LOCAL [{local}]\n'
    menu += f' 5.  Enable Subscription:    LOCAL [{local}]\n'
    menu += f' 6.  Level 1 Diagnostic:\n'
    menu += f' 7.  Level 2 Diagnostic:\n'
    menu += 'q to Quit\n'

    return menu


# REPLICATION MENU FUNCTIONS
def get_status(ip: str):
    # Gets the replication status

    # get db connection.
    db = database.get_db(ip)

    for i in db:
        data = i.execute("""SELECT * FROM replication_check_status() """).fetchall()

    return data


def get_sub_status(ip: str):
    # Gets the replication status and pulls out only the local ip and subscription info

    # get db connection
    db = database.get_db(ip)

    for i in db:
        data = i.execute("""SELECT * FROM replication_check_status() """).fetchall()

    data2 = []

    for d in data:
        if d["item"] == 'status' or d["item"] == 'local_ip':
            data2.append(d)

    return data2


def rebuild_subscriptions(local_ip: str, remote_ip: str):
    # Drop/Create local and remote subscriptions
    print("Working...")

    # drop subscriptions on primary and secondary servers
    drop_subscription(local_ip)
    drop_subscription(remote_ip)

    # create subscriptions on primary and secondary servers
    create_subscription(local_ip)
    create_subscription(remote_ip)

    # sleep to give subs time to initialize
    time.sleep(1)

    print("DONE.")

def check_replication():

    try:
        pass
    except Exception as err:
        pass
    else:
        pass


def rebuild_replication(local_ip: str, remote_ip: str, primary_ip: str, secondary_ip: str):
    # Removes replication subs, pubs, & init and rebuilds it.
    # This is handy if you're not sure what's wrong.

    print("Working...")

    # drop subscriptions on primary and secondary servers
    drop_subscription(local_ip)
    drop_subscription(remote_ip)

    # remove replication
    remove_replication(local_ip)
    remove_replication(remote_ip)

    # initialize on both servers
    initialize_replication(local_ip, primary_ip, secondary_ip)
    initialize_replication(remote_ip, primary_ip, secondary_ip)

    # create subscriptions on primary and secondary servers
    create_subscription(local_ip)
    create_subscription(remote_ip)

    time.sleep(1)
    print("DONE.")


def disable_subscription(ip: str):
    # get db connection
    db = database.get_db(ip)

    for i in db:
        data = i.execute("""SELECT * FROM replication_disable_simplicity_subscriptions() """).fetchall()

        return data


def enable_subscription(ip: str):
    # get db connection
    db = database.get_db(ip)

    # try:
    for i in db:
        data = i.execute("""SELECT * FROM replication_enable_simplicity_subscriptions() """).fetchall()

        return data


def diagnostic(local_ip: str, remote_ip: str, level: int=1, primary: str=None, secondary: str=None):
    pass


# HELPER FUNCTIONS
def drop_subscription(ip: str):
    # get db connection
    db = database.get_db(ip)

    # try:
    for i in db:
        data = i.execute("""SELECT * FROM replication_drop_simplicity_subscriptions() """).fetchall()

    return data


def create_subscription(ip: str):
    # get db connection
    db = database.get_db(ip)

    # try:
    for i in db:
        data = i.execute("""SELECT * FROM replication_create_simplicity_subscriptions() """).fetchall()

    return data


def remove_replication(ip: str):
    # Make sure subscriptions are dropped first.

    # TODO Ensure there are no subscriptions first

    # get db connection
    db = database.get_db(ip)

    # remove replication
    for i in db:
        data = i.execute("""SELECT * FROM replication_remove() """).fetchall()

    return data


def initialize_replication(ip: str, primary: str, secondary: str):
    # TODO figure out input parameters and check system_locals

    # get db connection
    db = database.get_db(ip)

    # remove replication
    for i in db:
        data = i.execute("""SELECT * FROM replication_config_init(%s,%s) """, (primary, secondary)).fetchall()

        return data


def get_local_ip():
    # get the local ip address
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Connect to machine accessible to all
    s.connect(("172.16.51.253", 80))
    lip = s.getsockname()[0]
    s.close()

    lip = '192.168.1.123'
    return lip


def get_system_replication(local_ip):
    # get the primary and secondary ip addresses from the system_replication table
    # get db connection
    db = database.get_db(local_ip)

    for i in db:
        # TODO MAKE A SQL FUNCTION INSTEAD OF THE ADHOC QUERY HERE.
        # TODO MAKE FUNCTION ENSURE TABLE CONTAINS THE LOCAL_IP
        data = i.execute("""SELECT * FROM replication_get_system_replication() """,).fetchall()
        # data = res = [ sub['host'] for sub in data ]

    return data


if __name__ == "__main__":
    main()
