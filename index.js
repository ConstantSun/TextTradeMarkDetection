const puppeteer = require('puppeteer-core');
var start = new Date();
const chromium = require('@sparticuz/chromium');

exports.handler = async (event, context, callback) => {
  var serial_no = event['serial_no'];                   // type:  number or string is ok .
  var valid_status_code = event['valid_status_code'];   // type:  must be an array of string .
  let mark_type_code = "";
  let is_status_valid = "status_valid_false";
  let browser = null;
  let goods_n_service = "";
  let res = [];

  try {
    browser = await puppeteer.launch({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: await chromium.executablePath,
      headless: chromium.headless,
      ignoreHTTPSErrors: true,
    });

    let page = await browser.newPage();

    var page_1 = "https://tsdr.uspto.gov/statusview/sn" + serial_no;
    await page.goto(page_1);


    const images = await page.evaluate(() => Array.from(document.images, e => e.src));
    img_src = images[1]
    start_index = img_src.indexOf("tm5/")+4
    end_index = img_src.indexOf(".png")
    status_code = img_src.substring(start_index, end_index)
    console.log("status_code :", status_code)
    if (valid_status_code.includes(status_code)){
      is_status_valid = "status_valid_true";
    }
    console.log("is_status_valid :", is_status_valid);
  
  
    class_key_value = await page.evaluate(() => {
      const elements = document.querySelectorAll('div.key, div.value');
      const list = [];
      for (const element of elements) {
        console.log("ele:")
        console.log(element)
        list.push(element.innerText);
      } 
      return list;
    });
  
    console.log("Mark Drawing Type:");
    const ind_marktype = class_key_value.indexOf('Mark Drawing Type:');
    mark_type_code = class_key_value[ind_marktype+1]
    console.log(mark_type_code);
  
    console.log("Usage:");  // goods_n_service
    const ind_usage = class_key_value.indexOf('For:')
    goods_n_service = class_key_value[ind_usage+1]
    console.log(goods_n_service)

  
    setTimeout(function (argument) {
      // execution time simulated with setTimeout function
      var end = new Date() - start
      console.info('Execution time: %dms', end)
    }, 0)


    res = [mark_type_code, is_status_valid, goods_n_service ]


  } catch (error) {
    return callback(error);
  } finally {
    if (browser !== null) {
      await browser.close();
    }
  }
  return callback(null, res);
};

