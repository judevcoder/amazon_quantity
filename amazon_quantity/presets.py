from amazon_quantity.utils import other_if_empty

site_xpath = {
    'captcha_form': '//form[@action="/errors/validateCaptcha"]',
    'captcha_image': "string(.//img[contains(@src, 'captcha')]/@src)",
    'add_to_card': '//input[@id="add-to-cart-button"]/@value',
    'link': 'string(//div[@id="mbc"]/div/div/div/span/a/@href)',  # WTF?
    'drop_down_asins': '//div[contains(@id, "variation_") and contains(@id, "_name")]//select/option[not(@value="-1")]/@value',
    'link_asins': '//div[contains(@id, "variation_") and contains(@id, "_name")]//ul/@data-defaultasin',
    'twister_asins': '//form[@id="twister"]//a[contains(@href,"/dp/")]/@href',

}
item_xpath = {
    'name': "string(.//*[@id='productTitle'])",
    'price': other_if_empty(
        other_if_empty(
            ".//*[@id='priceblock_saleprice']/text()",  # Sale https://www.amazon.com/dp/B06WW5HT8C?_encoding=UTF8&psc=1
            ".//*[@id='priceblock_ourprice']/text()",   # Price https://www.amazon.com/dp/B0120RRS1O?_encoding=UTF8&psc=1
        ),
        ".//li[@class='swatchSelect']//span[@class='a-size-mini']/text()"  # Multi https://www.amazon.com/dp/B00004YOHN/ref=twister_B01HT8TCOU?_encoding=UTF8&psc=1
    ),
    'stars': '//span[@id="acrPopover"]/span/a/i/span/text()',
    'color': '//div[@id="variation_color_name"]/div/span/text() | //span[@id="vodd-button-label-color_name"]/text() | //span[@class="shelf-label-variant-name"]/text()',
    'size': '//select[@id="native_dropdown_selected_size_name"]/option[@selected]/text() | //span[@id="vodd-button-label-size_name"]/text()',
    'reviews': '//span[contains(@class,"totalReviewCount")]/text()',
    'bsr': '//tr[contains(./th/text(), "Best Sellers Rank")]/td/span/span[1]/text()[normalize-space(.)] | //li[@id="SalesRank"]/text()[normalize-space(.)] | //li[@id="SalesRank"]/ul/li/span/text()',
    'seller': other_if_empty(
        '/*//a[@id="bylineInfo" or @id="brand"]/text()',
        '/*//a[@id="brand"]/@href'
    ),
}
product_xpath = {
    # 'quantity': 'string(//div[@id="hlb-subcart"]/div/span/span/text()[normalize-space(.)])',
    'quantity': "string(.//*[@id='hlb-ptc-btn-native']/text())",
}
offer_xpath = {
    'row': '//div[@role="main"]/div[contains(@class,"olpOffer")]',
    'seller': other_if_empty(
        'string(.//div[4]/h3/span/a/text())',
        'string(./div[4]/h3/a/img/@alt))',
    ),
    'price': other_if_empty(
        'string(./div[1]/span[contains(@class,"olpOfferPrice")]/text())',
        'string(0)',
    ),
    'stars': 'string(./div[4]/p/i/span/text())',
    'form': 'string(.//form//input[@name="submit.addToCart"]/@value)',
    'offering_id': 'string(.//form/input[contains(@name,"offeringID")]/@value)',
    'next_page': 'string(//li[@class="a-last"]/a/@href)'
}