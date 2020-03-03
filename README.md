Miranda / Miranda NG dbx_mmap database reader/writer
Reads and partially writes Miranda / Miranda NG dbx_mmap databases ("home.dat").

Can be used programmatically or from command-line.

Command-line quickstart:
  [*.py] --help

Programmatic quickstart:
  import mirandadb
  db = mirandadb.MirandaDbxMmap(filename)
  db.contacts()
  db.events()

See dbx_mmap format quick intro below.

This is a bit raw, feel free to improve or request missing functions.


## Command-line usage

### mirandadb.py
Dumps the contents of dbx_mmap database.

```
    dump-modules        prints all module names
    add-module          adds a new module to the database
    dump-contacts       prints contacts
    dump-settings       prints settings for the given contact
    event-stats         collects event statistics
    dump-events         prints all events for the given contacts
    dump-event          prints the specific events
    add-event           adds a simple message event to the end of the chain
    delete-event        deletes event at a given offset
```

Also can edit the database _a little bit_. Currently only module registration and adding/deleting events is supported.


### mirdiff.py
Compares two snapshots of **the same** Miranda database, looking for changed, added or deleted events (messages).

Useful to check if two snapshots have the same events, whether any events have been lost or added. Can be limited to only certain contacts.

Can **merge** modules, contacts and events from one snapshot to another (similar to how Miranda's "Import" function does it, but less guessmatching contacts => only matches contacts on the snapshots of the same database).

Note: Does not diff settings currently, mirevo.py is sufficient for that.


### mirevo.py
Loads all matching database snapshots one by one and traces data evolution through it.

When you have a lot of backups (home-2018, home-2019, home-2020), this will trace how all contacts and configuration settings have changed with time.
For instance how the contact information has changed with years.


### mirrestore.py
Scans the database and tries to find events/messages which might be corrupted (do not look like valid events).


## How to use to restore the database
As an example, let's fix some database corruption. Say you have an older snapshot with events until 2019.01 and current database from 2020.01, and in the meanwhile you've received some new messages but also some older messages became corrupt.

Use mirrestore.py --delete-extra to delete all messages from the newer database which are not found in the older database AND are not simply newer by date (in other words, corrupted versions of older messages).

Use mirdiff.py --merge-events to import all messages from the older database which are missing from the newer database (in other words, uncorrupted versions of older messages)

Verify that the old and the new versions differ only in entirely newer messages.


## dbx_mmap quickstart

Miranda database consists of the following things:

* Modules - basically just strings. Each module has a name and an offset in the database at which the name is stored. E.g.: "XMPP", "HistoryPlusPlus". Each contact is associated with a (protocol) module, each setting is associated with a module which placed it.

* Contacts - own properties/settings ("Nickname", "Hide in contact list", "Protocol") and their chain of messages. Each contact has their "protocol module" specified in the settings (see below)

* "Me" / System contact - owns common properties/settings and the chain of system messages

* Settings - Name/DataType/Value triplets, organized in DBContactSettings groups. Each group is attributed to one module. Basically DBContactSettings groups are folders and DBContactSetting items are entries in them.

* Events ~= messages and stuff like "%s have gone offline" "%s sends you a picture" etc. Organized in doubly-linked chains, each chain starts at some Contact and continues until NULL.

Each event has associated "module" (which generated it and can parse its data) and "contact" (to which it is attribute). These may differ from the chain which hosts the event! When MetaContacts are enabled, events from all subcontacts ("ChildA", proto: XMPP; "ChildB", proto: "ICQ") will be hosted in "ParentContact" chain (proto: "MetaContact").

This creates confusion so if you use high-level contacts() iter of mirandadb.MirandaDbxMmap, flags are set by default to hide this from you and iterate over the contact's events wherever they are stored. If you use low-level functions, you're on your own -- query get_meta_parent()/is_meta() to see if the DBContact needs special handling.


## Programmatic usage

See mirandadb's command line code for examples.

