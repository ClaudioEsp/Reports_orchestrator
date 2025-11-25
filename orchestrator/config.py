# config.py

# config.py

JOBS = [

    ############## GET PAST ROUTES #################
    {
        "id": "sync_routes_past",
        "module": "jobs.get_routes",
        "type": "cron",
        "cron": "5 2 * * *",   # 02:05 UTC every day
        "kwargs": {
            "days_back": 3,
            "minified": True,  # DON'T TOUCH
        },
        "run_at_startup": False,
    },

    ############## GET TODAY ROUTES #################
    {
        "id": "sync_routes_today",
        "module": "jobs.get_routes",
        "type": "cron",
        "cron": "5 2 * * *",   # 02:05 UTC every day
        "kwargs": {
            "days_back": 0,    # today
            "minified": True,  # DON'T TOUCH
        },
        "run_at_startup": False,
    },

    ############## EXTRACT DISPATCHES FROM ROUTES #################
    {
        "id": "extract_dispatchets",
        "module": "jobs.get_dispatches",
        "type": "interval",
        "minutes": 1, 
        "kwargs": {
            "days_back": 3,    # today
        },
        "run_at_startup": False,
    },

    ############## EXTRACT DISPATCHES FROM ROUTES #################
    {
        "id": "sync_dispatches_details",
        "module": "jobs.get_dispatches_details",
        "trigger": "interval",
        "type": "interval",
        "minutes": 1,
        "kwargs": {
            "limit": 100,
            "throttle_seconds": 0.2,
        },
        "run_at_startup": False,
    },
    ############## EXTRACT DISPATCHES FROM ROUTES #################
    {
        "id": "get_ct",
        "module": "jobs.backfill_ct_for_dispatches",
        "trigger": "interval",
        "type": "interval",
        "minutes": 1,
        "run_at_startup": False,
    },
    {
        "id": "get_substatus",
        "module": "jobs.backfill_substatus_for_dispatches",
        "trigger": "interval",
        "type": "interval",
        "minutes": 1,
        "run_at_startup": True,
    },


    ############################## NO BEETRACK ##############################


    ############## GET SUBSTATUS #################

]
