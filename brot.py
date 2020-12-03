import matplotlib.pyplot as plt
import numpy as np

def mandelbrot_set(width, height, zoom=1, x_off=0, y_off=0, niter=256):
    w,h = width, height
    pixels = np.arange(w*h, dtype=np.uint16).reshape(h, w)

    for x in range(w): 
        zx = 1.5*(x + x_off - 3*w/4)/(0.5*zoom*w)
        for y in range(h):
            
            zy = 1.0*(y + y_off - h/2)/(0.5*zoom*h)
            
            z = complex(zx, zy)
            c = complex(0, 0)
            
            for i in range(niter):
                if abs(c) > 4: break
                c = c**2 + z

            color = (i << 21) + (i << 10)  + i * 8
            pixels[y,x] = color
  
    return pixels

def display(width=1024, height=768, zoom=1.0, x_off=0, y_off=0, cmap='viridis'):

    pixels = mandelbrot_set(width, height, zoom=zoom, x_off=x_off, y_off=y_off)
    # print(pixels.tobytes())
    # Let us turn off the axes
    plt.axis('off')
    plt.imshow(pixels, cmap=cmap)
    plt.savefig("result.png")

import time
start = time.time()

display(width=1024, height=768, zoom=1.0)

end = time.time()
print(end - start)