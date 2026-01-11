class UrlBuilder:
    def __init__(self):
        pass

    def build_search_url(self, query, page_num=1, sort=None, filters=None,phone_model=None):
        if phone_model:
            if page_num == 1:
                url = f"https://www.olx.pl/elektronika/q-{query}/?search%5Bfilter_enum_phonemodel%5D%5B0%5D={phone_model}&search%5Border%5D=created_at%3Adesc"
            url = f"https://www.olx.pl/elektronika/q-{query}/?page={page_num}&search%5Bfilter_enum_phonemodel%5D%5B0%5D={phone_model}&search%5Border%5D=created_at%3Adesc"
        else: 
            url = f"https://olx.pl/q-{query}/?page={page_num}"
        return url
    
    def build_product_url(self,product_link):
        url = f"https://olx.pl{product_link}"
        return url