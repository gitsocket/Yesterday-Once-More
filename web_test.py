#!/usr/bin/env python
#coding=utf-8

import web
from web import form

from customize import Customizer

urls = (
        '/', 'index',
        '/yesterday', 'yesterday',
        )
app = web.application(urls, globals())
render = web.template.render('templates/')

content = {}

class yesterday:
    def GET(self):
        data = web.input(p=0)
        user_id = int(data.id)
        p = int(data.p)
        if user_id not in content:
            customizer = Customizer()
            ctx = customizer(user_id)
            content[user_id] = ctx
        else:
            ctx = content[user_id]
        output = {}
        output['date'] = ctx['date']
        output['recommendations'] = ctx['recommendations'][p:p+10]
        return render.yesterday(output,user_id,p+10)

class index:
    def GET(self):
        user_id = form.Form(form.Textbox('id'))
        f = user_id()
        return render.index(f)

if __name__ == "__main__":
    app.run()
