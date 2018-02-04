import pandas as pd
from App.forecasting_followers import analyze_forecast

def ModelIt(user_id  = 'Default'):
    
    if user_id != 'Default':
        result = analyze_forecast(user_id)
        result['pred_total'] = result['pred_total'].astype(int)
        return result
    else:
        print('you are using defualt parameters')
        dummy_result = {'actual_total':100, 'pred_total':199,'hours_streamed':56}
        default_result = pd.DataFrame(dummy_result, index = range(0,1))
	
        return default_result


def get_image_url(user_id):
    
    url = '../static/{}.png'.format(user_id)
    
    return url
