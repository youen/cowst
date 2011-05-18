$.widget("ui.cowst_widget", 
{

_init:function()
	  {
		   dj.remote('djity_cowst.example_ajax',
		   {
				js_target:this,
				text:this.element.text(),
			});
	   },
text : function(text)
		//callback function
	   {
		   this.element.text('from ajax: ' +text);
	   }
});

