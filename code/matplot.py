from pylab import *

figure(figsize=(3,4))

y=[pow(10,i) for i in range(-2,2)]
x=range(0,len(y))
# subplot(3,3)
title("yscale('log')")
# yscale('log')
plot(x,y)
show()