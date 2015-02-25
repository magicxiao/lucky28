var refContentDef = "请输入关键字";

// 初始化
$(function (){
	// 导航菜单子菜单
	$("ul.topnav li.b").mouseover(function() {
		
		$(this).find("ul.subnav").slideDown('fast').show();

		$(this).hover(function() {
		}, function(){	
			$(this).find("ul.subnav").slideUp('slow');
		});

		}).hover(function() { 
			$(this).addClass("subhover");
		}, function(){
			$(this).find("ul.subnav").stop(true,true).slideToggle();
			$(this).removeClass("subhover");
	});


	try {
		// 初始化搜索框
		RefFormWord();
		$('#refContent').blur(function (){
			RefFormWord();
		});
		$('#refContent').click(function (){
			RefFormNoWord();
		});
	}catch (e) {}


	// 加载蒙层窗口
	CheckJsPopup();
});


// 显示默认值
function RefFormWord(){
	if ($id('refContent').value == ''){
		$id('refContent').value = refContentDef;
		$id('refContent').style.color = '#a59ea3';
	}
}
// 不显示默认值
function RefFormNoWord(){
	if ($id('refContent').value == refContentDef){
		$id('refContent').value = '';
		$id('refContent').style.color = '#000000';
	}
}

// 查询表单检测
function CheckRefForm(){
	refContStr = $id("refContent").value;
	if (refContStr == '' || refContStr == refContentDef){
		alert("请输入要搜索的关键字");
		//$id("refContent").value='';$id("refContent").focus();
		return false;
	}
	switch ($id("refMode").value){
		case "theme": case "content": case "source": case "writer": 
			$.ajaxSetup({cache:false});
			$.get(dbPathPart +"read.asp?mudi=getUrlencode&str="+ escape(refContStr), function(result){
				refContEncodeStr=result;
				document.location.href=dbPathPart +"newsList.asp?typeStr=refer&refType="+ $id("refMode").value +"&refContent="+ refContEncodeStr;
			});
			break;
	
		case "taobao":
			var a=window.open("http://z.alimama.com/tksEncrypt.php?q="+ refContStr +"&cat=0&pid=mm_16070858_0_0&unid=&commend=all&search_type=auction&user_action=initiative&f=D9_5_1&at_topsearch=1&sid=%2810742bc12b9bb7514f168aa823f34e31%29&sort=&spercent=0&st=0");
			break;
	
		case "360buy":
			var a=window.open("http://click.union.360buy.com/JdClick/?keyword="+ refContStr +"&unionId=22133&t=5&to=http%3A%2F%2Fsearch.360buy.com%2FSearch");
			break;
	
		case "dangdang":
			var a=window.open("http://union.dangdang.com/transfer.php?dd_key="+ refContStr +"&backurl=http%3A%2F%2Fsearch.dangdang.com%2Fsearch.php%3Fkey%3D"+ refContStr +"&ad_type=40&sys_id=1&from=P-279550");
			break;
	
		case "joyo":
			var a=window.open("http://www.amazon.cn/gp/associates/link-types/searchbox.html?tag=sunyi3210-23&creative=2032&adid=09F5R0TTE1XK7M4Q5V70&campaign=408&mode=blended&keyword="+ encodeURI(refContStr) +"");
			break;
	
		case "baidu":
			var a=window.open("http://www.baidu.com/baidu?word="+ encodeURI(refContStr) +"&tn=sunyi3210&fyb=0&tr=mk3SLVN4HKm&ie=utf-8");
			break;
	
		case "google":
			var a=window.open("http://www.google.com/cse?cx=partner-pub-5587033679474810%3Ahhvo4r1heos&ie=UTF-8&q="+ encodeURI(refContStr));
			break;

		case "gougou":
			var a=window.open("http://web.gougou.com/search?search="+ encodeURI(refContStr) +"&restype=-1&id=106428&pattern=10000");
			break;

		case "xunlei":
			var a=window.open("http://search.xunlei.com/search.php?keyword="+ refContStr +"&id=23106329");
			break;

		case "vancl":
			var a=window.open("http://s.vancl.com/search?source=sunyi3210&k="+ encodeURI(refContStr));
			break;

		case "mbaobao":
			mbaobaoStr = "http://search.mbaobao.com/searcher?k="+ refContStr;
			$.ajaxSetup({cache:false});
			$.get(dbPathPart +"read.asp?mudi=getUrlencode&str="+ escape(mbaobaoStr), function(result){
				mbaobaoStr=result;
				var a=window.open("http://cl.mbaobao.com/?url="+ mbaobaoStr +"&cps_cpid=6074&cps_adid=653");
			});
			break;
	
	}
	return false;
}

function Weather114la(num){
	document.write("<iframe width='540' height='22' frameborder='0' scrolling='no' allowtransparency='true' src='"+ dbPathPart +"tools/weather/index"+ num +".html'></iframe>");
}

// 是否更新
if (todayDate!=lastDate){
	AjaxGetDeal(dbPathPart +"configDeal.asp?isAutoHomeHtml="+ isAutoHomeHtml);
}


var isJsPopup=false,isJsNicEdit=false,isJsUsers=false,isJsUsersCenter=false;

// popup加载
function CheckJsPopup(){
	if (isJsPopup==false){
		LoadJsFile("popupJs",dbPathPart +"tools/popup/popup.js",0);
		LoadCssFile("popupCss",dbPathPart +"tools/popup/popup.css",0);
		LoadCssFile("popupStyleCss",dbPathPart +"tools/popup/skin/"+ SYS_skinPopup +"/style.css",0);
	isJsPopup=true;
	}
}

// nicEdit编辑器加载
function CheckJsNicEdit(){
	if (isJsNicEdit==false){
//		LoadJsFile("nicEditJs",dbPathPart +"tools/nicEdit/nicEdit.js",0);
		LoadJsFile("kindeditorJs",dbPathPart +"tools/kindeditor/kindeditor-min.js",0);
	isJsNicEdit=true;
	}
}

// 会员JS加载
function CheckJsUsers(){
	if (isJsUsers==false){
		LoadJsFile("usersJs",dbPathPart +"js/users.js",0);
		LoadJsFile("usersCenterJs",dbPathPart +"js/usersCenter.js",0);
	isJsUsers=true;
	}
}

