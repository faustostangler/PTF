import dash
import dash_bootstrap_components as dbc

# Initialize the Dash app with optional external stylesheets
# Here we are using a Bootstrap theme for styling
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Assuming your app will be deployed, setting this to your app's name
app.title = 'Análise Fundamentalita by FS'

# Configuring server for deployment
server = app.server

# Ensure this for enabling callbacks to work on components that are not immediately loaded into the layout
app.config.suppress_callback_exceptions = True

if __name__ == '__main__':
    app.run_server(debug=True)
