source("RemembR/R/RemembeR.R")
example.wiki <- 'enwiki'
models.dir <- 'models'
print("mod.adoption")
source("analyze_threshold_models.R")

analyze.model(file.path(models.dir,"adoption.check.vlb.stanmod.RDS"),"mod.adoption.vlb",'enwiki',0.03,0)

#2 
print("mon.anon.reverted")
analyze.model(file.path(models.dir,"anon.is.reverted.vlb.stanmod.RDS"),"mod.anon.reverted.vlb",example.wiki,0.03,0)

#3
print("mod.non.anon.reverted")
analyze.model(file.path(models.dir,"non.anon.is.reverted.vlb.stanmod.RDS"),'mod.non.anon.reverted.vlb',example.wiki,0.03,0)

#4 
print("mod.no.user.page.reverted")
analyze.model(file.path(models.dir,"no.user.page.is.reverted.vlb.stanmod.RDS"),'mod.no.user.page.reverted.vlb',example.wiki,0.03,0)

#5
print('mod.user.page.reverted')
analyze.model(file.path(models.dir,"user.page.is.reverted.vlb.stanmod.RDS"),'mod.user.page.reverted.vlb',example.wiki,0.03,0)

#7
print('mod.non.anon.controversial')
analyze.model(file.path(models.dir,"non.anon.is.controversial.vlb.stanmod.RDS"),'mod.non.anon.controversial.vlb',"all",0.03,0)

#8
print('mod.anon.controversial')
analyze.model(file.path(models.dir,"anon.is.controversial.vlb.stanmod.RDS"),'mod.anon.controversial.vlb',example.wiki,0.03,0)

#9
print('mod.user.page.controversial')
analyze.model(file.path(models.dir,"user.page.is.controversial.vlb.stanmod.RDS"),'mod.user.page.controversial.vlb',example.wiki,0.03,0)

#10
print('mod.no.user.page.controversial')
analyze.model(file.path(models.dir,"no.user.page.is.controversial.vlb.stanmod.RDS"),'mod.no.user.page.controversial.vlb',example.wiki,0.03,0)
