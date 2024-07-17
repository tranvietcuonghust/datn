class Format():
    def price_format(self, price):
        # price = float(price)
        # price_str = f'{price:,}'
        price_str = price
        price_str = price_str + " VNĐ"
        return price_str

    def area_format(self, area):
        area_str = str(area) + ' m2'
        return area_str

    def short_title(self, title):
        if len(title) < 100:
            return title
        else:
            return title[:97] + "..."

    def short_address(self, address):
        if len(address) <= 80:
            ps = 80 - len(address)
            for i in range(ps):
                address += "&nbsp"
            return address
        else:
            return address[:77] + "..."

    def address_format(self, address):
        address_str = address.replace(" ", "+")
        return address_str
