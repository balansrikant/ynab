from flask import render_template, Blueprint

ynab_bp = Blueprint('ynab_bp', __name__,
                    template_folder='templates',
                    static_folder='static')


@ynab_bp.route('/')
@ynab_bp.route('/index')
def welcome():
    user = {'username': 'Srikant'}
    return render_template('index.html', title='YNAB', user=user)
