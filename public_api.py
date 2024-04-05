import tornado.web
import tornado.log
import tornado.options
import logging
import json
import requests

class App(tornado.web.Application):

    def __init__(self, handlers, **kwargs):
        super().__init__(handlers, **kwargs)

class BaseHandler(tornado.web.RequestHandler):
    def write_json(self, obj, status_code=200):
        self.set_header("Content-Type", "application/json")
        self.set_status(status_code)
        self.write(json.dumps(obj))

# /users
class UsersHandler(BaseHandler):
    def initialize(self, user_service_url):
        self.user_service_url = user_service_url

    @tornado.gen.coroutine
    def post(self):
        req_body = self.request.body

        # Parsing JSON request body
        data = json.loads(req_body)
        user_name = data.get("name")
        
        # Building request
        payload = {"name": user_name}

        # Making api call
        response = requests.post(self.user_service_url+"/users", params=payload)

        self.write_json(response.json())

# /listings
class ListingsHandler(BaseHandler):
    def initialize(self, listing_service_url):
        self.listing_service_url = listing_service_url

    @tornado.gen.coroutine
    def get(self):
        # Parsing pagination params
        page_num = self.get_argument("page_num", 1)
        page_size = self.get_argument("page_size", 10)
        try:
            page_num = int(page_num)
        except:
            logging.exception("Error while parsing page_num: {}".format(page_num))
            self.write_json({"result": False, "errors": "invalid page_num"}, status_code=400)
            return

        try:
            page_size = int(page_size)
        except:
            logging.exception("Error while parsing page_size: {}".format(page_size))
            self.write_json({"result": False, "errors": "invalid page_size"}, status_code=400)
            return

        # Parsing user_id param
        user_id = self.get_argument("user_id", None)
        if user_id is not None:
            try:
                user_id = int(user_id)
            except:
                self.write_json({"result": False, "errors": "invalid user_id"}, status_code=400)
                return

        # Building request
        payload = {"page_num": page_num, "page_size": page_size, "user_id": user_id}
        response = requests.get(self.listing_service_url+"/listings", params=payload)
        data = response.json()
        
        # Fetching listings from listings service
        listings = []
        for row in data.get("listings"):
            fields = ["id", "user_id", "listing_type", "price", "created_at", "updated_at"]
            listing = {
                field: row[field] for field in fields
            }
            listings.append(listing)

        self.write_json({"result": True, "listings": listings})

    @tornado.gen.coroutine
    def post(self):
        req_body = self.request.body

        # Parsing JSON request body
        data = json.loads(req_body)
        user_id = data.get("user_id")
        listing_type = data.get("listing_type")
        price = data.get("price")

        # Validating request fields
        errors = []
        user_id_val = self._validate_user_id(user_id, errors)
        listing_type_val = self._validate_listing_type(listing_type, errors)
        price_val = self._validate_price(price, errors)
        
        # End if we have any validation errors
        if len(errors) > 0:
            self.write_json({"result": False, "errors": errors}, status_code=400)
            return

        # Building request
        payload = {"user_id": user_id_val, "listing_type": listing_type_val, "price": price_val}

        # Making api call
        response = requests.post(self.listing_service_url+"/listings", params=payload)

        self.write_json(response.json())

    def _validate_user_id(self, user_id, errors):
        try:
            user_id = int(user_id)
            return user_id
        except Exception as e:
            logging.exception("Error while converting user_id to int: {}".format(user_id))
            errors.append("invalid user_id")
            return None

    def _validate_listing_type(self, listing_type, errors):
        if listing_type not in {"rent", "sale"}:
            errors.append("invalid listing_type. Supported values: 'rent', 'sale'")
            return None
        else:
            return listing_type

    def _validate_price(self, price, errors):
        # Convert string to int
        try:
            price = int(price)
        except Exception as e:
            logging.exception("Error while converting price to int: {}".format(price))
            errors.append("invalid price. Must be an integer")
            return None

        if price < 1:
            errors.append("price must be greater than 0")
            return None
        else:
            return price

# /ping
class PingHandler(tornado.web.RequestHandler):
    @tornado.gen.coroutine
    def get(self):
        self.write("pong!")

def make_app(options):
    return App([
        (r"/ping", PingHandler),
        (r"/users", UsersHandler, dict(user_service_url=options.user_service_url)),
        (r"/listings", ListingsHandler, dict(listing_service_url=options.listing_service_url)),
    ], debug=options.debug)

if __name__ == "__main__":
    # Define settings/options for the web app
    # Specify the port number to start the web app on (default value is port 6000)
    tornado.options.define("port", default=6000)
    # Specify whether the app should run in debug mode
    # Debug mode restarts the app automatically on file changes
    tornado.options.define("debug", default=True)

    # Define host url of user service api 
    tornado.options.define("user_service_url", type=str)
    # Define host url of listing service api 
    tornado.options.define("listing_service_url", type=str)

    # Read settings/options from config file
    tornado.options.parse_config_file("public_api.conf")

    # Read settings/options from command line
    tornado.options.parse_command_line()

    # Access the settings defined
    options = tornado.options.options

    # Create web app
    app = make_app(options)
    app.listen(options.port)
    logging.info("Starting public api. PORT: {}, DEBUG: {}".format(options.port, options.debug))

    # Start event loop
    tornado.ioloop.IOLoop.instance().start()
