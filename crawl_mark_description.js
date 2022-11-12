const puppeteer = require('puppeteer');
var start = new Date();
var simulateTime = 10000;




(async () => {
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/chromium-browser'
  });
  const page = await browser.newPage();
  var page_url = "https://tsdr.uspto.gov/statusview/sn90127872"
  var page_1 = "https://tsdr.uspto.gov/statusview/sn85648469"
  await page.goto(page_1);

  // const data = await page.evaluate(() => {
  //   const tds = Array.from(document.querySelectorAll('table tr td'))
  //   return tds.map(td => td.innerText)
  // });

  const paragraph = await page.evaluate(() => {

    return  document.getElementById("markInfo-section").innerHTML;

  });

  // console.log(paragraph) 
  console.log("\n----\n")
  var substring = "Mark Drawing Type"
  var index_marktype = paragraph.indexOf(substring) + 61
  var index_div = paragraph.substring(index_marktype).indexOf("</div>")
  var result_type = paragraph.substring(index_marktype, index_marktype+index_div)
  console.log(result_type)

  // var substring = "Standard Character Claim"
  // var index_char_claim = paragraph.indexOf(substring) + 55+14
  // // console.log(paragraph.substring(index_char_claim, index_char_claim + 50))
  // var index_div = paragraph.substring(index_char_claim).indexOf("</div>")
  // var result_claim = paragraph.substring(index_char_claim, index_char_claim+index_div)
  // console.log(result_claim);

  setTimeout(function (argument) {
    // execution time simulated with setTimeout function
    var end = new Date() - start
    console.info('Execution time: %dms', end)
  }, 0)

  await browser.close();
})();
