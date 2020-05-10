import datetime
def date_part(val, part):
    if part == 'day':
        return val.day
    elif  part =='month':
        return val.month
    elif part == 'year':
        return val.year
    elif part == 'dow':
        return val.weekday() #0 is Monday and 6 is Sunday... weird.
    elif part == 'eom':
        return (date_trunc(val + datetime.timedelta(days = 32),'month') - datetime.timedelta(days = 1)).day

def date_trunc(val, part):
    if part == 'week':
        return val - datetime.timedelta(days = date_part(val,'dow'))
    elif part == 'month':
        return val - datetime.timedelta(days = date_part(val, 'day')-1)
    elif part == 'day':
        return val.replace(hour = 0, minute = 0, second = 0, microsecond = 0)