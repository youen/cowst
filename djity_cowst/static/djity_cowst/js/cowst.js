dj.widgets.cowst_import =
{
    /*
     * Import tools for SWOCT
     */

	
    init : function()
    {
		

		this.reader = new FileReader();
		this.pb = $('<div/>');
		this.message = $('<span/>');
		

		this.file_el = $('#file')
			.after(this.pb)
			.before(this.message)
			.change(function(e)
				{
					file = e.target.files[0];
					dj.widgets.cowst_import.reader.readAsText(file);
				});

		this.reader.onload = function(e)
			{
				dj.widgets.cowst_import.load(e.target.result)
				
			}
	},


	filter_properties : function(data)
	{
		var result = [];
		$.each(data,function(i,r)
		{
				p = dj.widgets.cowst_import.parse_property(r);
				if(p.nberror > 0)
				{
					result.push(p);
				}
		});
		return result;



	},

	parse_property : function(p)
	{
		function parse_bag(b)
		{
			tmp = b.split('),(');
			var results = [];
			$.each(tmp,function(i,e)
			{
				var result = [];
				t = e.split(',',2);
				result[0] = t[0].replace('{','').replace('(','').replace('<','')
								.replace('}','').replace(')','').replace('>','');
				result[1] = parseInt(
							t[1].replace('{','').replace('(','').replace('<','')
								.replace('}','').replace(')','').replace('>','')
							);
				results.push(result)
			});
			return results;
		}

		function parse_bag2(b)
		{
			tmp = b.split('),(');
			var results = [];
			$.each(tmp,function(i,e)
			{
				var result = [];
				t = e.split(',',2);
				result[0] = t[0].replace('{','').replace('(','').replace('<','')
								.replace('}','').replace(')','').replace('>','');
				result[1] =	t[1].replace('{','').replace('(','').replace('<','')
								.replace('}','').replace(')','').replace('>','');
				results.push(result)
			});
			return results;
		}
			prop = {};
			prop.uri = 	p[0].substr(1,p[0].length-2);
			prop.def = [];
			def = p[1].split(',',2);
			prop.def[0] = 	def[0].replace('{(','').replace('>','').replace('<','');
			prop.def[1] =	parseInt(def[1].replace(')}',''));
			alpha = p[2].split(',',2);
			prop.alpha = [];
			prop.alpha[0] = 	alpha[0].replace('{','').replace('(','').replace('<','')
										.replace('}','').replace(')','').replace('>','');
			prop.alpha[1] =	parseInt(alpha[1].replace(')}',''));
			prop.nberror = p[4];
			prop.def = parse_bag(p[1]);
			prop.repartition = parse_bag(p[5]);
			prop.hierarchy = parse_bag2(p[6]);
			return prop;
	},

	_add_property : function(prop)
	{
			var self=this;
			prop_header = $('<h3/>');
			$('<a/>')
				.appendTo(prop_header)
				.uri({uri:prop.uri});

			self.element.next()
				.append(prop_header);
			
			prop_header.after(
				$('<div/>')
					.property(prop)
				);
	},

	load : function(result)
	{

		this.properties = this.filter_properties($.csv('\t')(result));
		this.index_next =0;

		this.file_el
			.hide();
		
		this.pb
			.progressbar();

		this.next_upload();
	},

	next_upload:function()
	{
		
		if (this.index_next < this.properties.length)
		{
			prop = this.properties[this.index_next];
			dj.remote('djity_cowst.add_template',
				{
					'js_target':this,
					'uri':prop.uri,
					'alpha':prop.alpha,
					'histogram':prop.repartition,
					'hierarchy':prop.hierarchy,
				});
			this.index_next ++;
			this.pb.progressbar('value',(100 *this.index_next) / this.properties.length);
			this.message.text('uploading error for : ' + prop.uri);
		}
		else
		{
			location.reload();
		}
	}


};

