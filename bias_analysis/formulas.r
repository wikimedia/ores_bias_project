treatment.rhs <- . ~  user.revert.cv + ttr + n.reverts + p.reverts.admin + p.reverts.bot + p.reverts.patroller + p.reverteds.newcomer + p.reverteds.anonymous + ns0.anon.edits + ns0.newcomer.edits + ns4.edits + p.ns4.edits.newcomer + p.ns4.edits.anon + ns0.edits + p.ns0.edits.anon +  p.ns0.edits.newcomer + n.pages.baseline.ns.0 + n.pages.baseline.ns.4 +  active.editors + view.count + has.patrollers

treatment.lhs <- treated ~ .

treatment.formula<- update(treatment.rhs,treatment.lhs)

m1.rhs  <-  . ~  treated + week.factor + treated.with.ores 

m1.rhs.stan  <-  . ~ treated + week.factor + treated.with.ores + (1 | wiki.db)

m1.lhs <- undo.geom.mean.ttr ~ .
m1.formula <- update(m1.lhs,m1.rhs)
m1.formula.stan  <- update(m1.lhs, m1.rhs.stan)
m1.formula.dr <- update(m1.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m1.rhs[3],"- ttr",collapse="+")))

m1.formula.dr.stan <- update(m1.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m1.rhs[3],"- ttr + (1|wiki.db)",collapse="+")))

m2.lhs <- undo.N.reverts ~ .
m2.rhs <- m1.rhs
m2.rhs.stan <- m1.rhs.stan
m2.formula  <- update(m2.lhs, m2.rhs)
m2.formula.stan  <- update(m2.lhs, m2.rhs.stan)

m2.formula.dr <- update(m2.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m2.rhs[3],"- n.reverts",collapse="+")))

m2.formula.dr.stan <- update(m2.rhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m2.lhs[3],"- n.reverts + (1|wiki.db)",collapse="+")))

m3.lhs <- user.week.revert.cv ~ .
m3.rhs <- m1.rhs
m3.rhs.stan <- m1.rhs.stan
m3.formula <- update(m3.lhs,m3.rhs)
m3.formula.stan <- update(m3.lhs, m3.rhs.stan)

m3.formula.dr <- update(m3.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m3.rhs[3],"- user.revert.cv",collapse="+")))
m3.formula.dr.stan <- update(m3.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m3.rhs[3],"- user.revert.cv + (1|wiki.db)",collapse="+")))

m4.lhs <- newcomer.n.reverted ~ .
m4.rhs <- m1.rhs
m4.rhs.stan <- m1.rhs.stan
m4.formula <- update(m4.lhs, m4.rhs)
m4.formula.stan <- update(m4.lhs, m4.rhs.stan)
m4.formula.dr <- update(m4.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m4.rhs[3],"- p.reverteds.anonymous - p.reverteds.newcomer - p.reverteds.established",collapse="+")))
m4.formula.dr.stan <- update(m4.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m4.rhs[3],"- p.reverteds.anonymous - p.reverteds.newcomer - p.reverteds.established + (1|wiki.db)",collapse="+")))

m5.lhs <- anonymous.n.reverted ~ .
m5.rhs <- m1.rhs
m5.rhs.stan <- m1.rhs.stan
m5.formula <- update(m5.lhs, m5.rhs)
m5.formula.stan <- update(m5.lhs, m5.rhs.stan)
m5.formula.dr <- update(m5.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m5.rhs[3],"- p.reverteds.anonymous - p.reverteds.newcomer - p.reverteds.established",collapse="+")))
m5.formula.dr.stan <- update(m5.lhs, paste(". ~ + ", paste0(treatment.rhs[3],"+"),paste0(m5.rhs[3],"- p.reverteds.anonymous - p.reverteds.newcomer - p.reverteds.established + (1|wiki.db)",collapse="+")))
