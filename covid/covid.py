from flask import Flask, redirect, url_for, request, render_template
from google.cloud import bigquery

app = Flask(__name__)


@app.route('/')
def index():
    return render_template('asd.html',tittle="Home page")

@app.route('/get-confirmed-cases')
def case1():
    country = request.args.get('country')
    date = request.args.get('date')
    state = request.args.get('state')
    print(country,date)
    client = bigquery.Client()
    if state == 'None':
        query1 = """
        SELECT date, sum(confirmed) as total_cases_till_date
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and date = @date GROUP BY date
        """
    else:
        query1 = """
        SELECT date, sum(confirmed) as total_cases_till_date
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and province_state = @state and date = @date GROUP BY date
        """

    config = bigquery.QueryJobConfig(
                query_parameters=[
                        bigquery.ScalarQueryParameter("country", "STRING", country),
                        bigquery.ScalarQueryParameter("date", "DATE", date),
                        bigquery.ScalarQueryParameter("state", "STRING", state)
                        ]
                        )
    query_res = client.query(query1,job_config=config)
    #print(query_res)
    results = {}
    for row in query_res:
        results[str(row.date)] = row.total_cases_till_date

    #print(row.date,row.total_cases_till_date,query_res)
    if state == "None":
        return (results)#"%s Country has %d Number of Confirmed cases till %s" %(country,row.total_cases_till_date,date)
    else:
        return (results)#"%s State in %s Country has %d Number of Confirmed cases till %s" %(state,country,row.total_cases_till_date,date)


@app.route('/get-confirmed-cases-between/')
def case2():
    country = request.args.get('country')
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    state = request.args.get('state')

    client = bigquery.Client()
    if state == 'None':
        query2 = """
        SELECT date, sum(confirmed) total_cases_to_date
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and date >= @from_date  and date <= @to_date
        GROUP BY date
        ORDER BY date
        """
    else:
        query2 = """
        SELECT date, sum(confirmed) total_cases_to_date
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and province_state = @state and date >= @from_date and date <= @to_date
        GROUP BY date
        ORDER BY date
        """

    config = bigquery.QueryJobConfig(
                query_parameters=[
                        bigquery.ScalarQueryParameter("country", "STRING", country),
                        bigquery.ScalarQueryParameter("from_date", "DATE", from_date),
                        bigquery.ScalarQueryParameter("to_date", "DATE", to_date),
                        bigquery.ScalarQueryParameter("state", "STRING", state)
                        ]
                        )
    query_res = client.query(query2,job_config=config)
    #print(query_res)
    results = {}
    for row in query_res:
        results[str(row.date)] = row.total_cases_to_date
    return results

@app.route('/get-average-number-of-cases-perday')
def case3():
    country = request.args.get('country')
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    state = request.args.get('state')

    client = bigquery.Client()

    if state == 'None':
        query3 = """
        DECLARE cases_till_x INT64;
        DECLARE cases_till_y INT64;

        DECLARE days_diff INT64;
        SET days_diff = ABS(( SELECT DATE_DIFF(@from_date ,@to_date , DAY) ));

        SET cases_till_x = ((
        SELECT sum(confirmed)
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and date = @from_date
        GROUP BY date));

        SET cases_till_y = ((
        SELECT sum(confirmed)
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and date = @to_date
        GROUP BY date));

        SELECT DIV((cases_till_y-cases_till_x),days_diff) AS result;
        """
    else:
        query3 = """
        DECLARE cases_till_x INT64;
        DECLARE cases_till_y INT64;

        DECLARE days_diff INT64;
        SET days_diff = ABS(( SELECT DATE_DIFF(@from_date ,@to_date , DAY) ));

        SET cases_till_x = ((
        SELECT sum(confirmed)
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and province_state = @state and date = @from_date
        GROUP BY date));

        SET cases_till_y = ((
        SELECT sum(confirmed)
        FROM `project-shared-312909.covid.covid_world`
        WHERE country_region = @country and province_state = @state and date = @to_date
        GROUP BY date));

        SELECT DIV((cases_till_y-cases_till_x),days_diff) AS result;
        """


    config = bigquery.QueryJobConfig(
                query_parameters=[
                        bigquery.ScalarQueryParameter("country", "STRING", country),
                        bigquery.ScalarQueryParameter("from_date", "DATE", from_date),
                        bigquery.ScalarQueryParameter("to_date", "DATE", to_date),
                        bigquery.ScalarQueryParameter("state", "STRING", state)
                        ]
                        )
    query_res = client.query(query3,job_config=config)
    results = {}
    for row in query_res:
        results[country] = str(row.result)
        #output = str(row.result)
    if state == "None":
        return results#"Average number of cases between %s and %s dates in %s Country are : %s " %(from_date,to_date,country,output)
    else:
        #print(state,country,output)
        return results#"Average number of cases between %s and %s dates in %s State %s Country are : %s " %(from_date,to_date,state,country,output)

@app.route('/login',methods = ['POST', 'GET'])
def login():
    if request.method == 'POST':
        if request.form['swcase'] == '1':
            country = request.form['country_name']
            date = request.form['to_date']
            state = request.form['state_name']
            return redirect(url_for('case1',country = country,date = date,state = state))
        elif request.form['swcase'] == '2':
            country = request.form['country_name']
            state = request.form['state_name']
            from_date = request.form['from_date']
            to_date = request.form['to_date']
            return redirect(url_for('case2',country = country, state = state,from_date = from_date, to_date=to_date))
        else:
            country = request.form['country_name']
            state = request.form['state_name']
            from_date = request.form['from_date']
            to_date = request.form['to_date']
            return redirect(url_for('case3',country = country,state = state,from_date = from_date, to_date=to_date))


if __name__ == '__main__':
    app.run(host="127.0.0.1", port=8080,debug = True)
