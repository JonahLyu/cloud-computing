import matplotlib.pyplot as plt
import numpy as np
import math

def mandelbrotSet(width, height, zoom=1, niter=256):
    w,h = width, height
    pixels = np.arange(w*h, dtype=np.uint16).reshape(h, w)
    zoom = 1 / zoom
    for x in range(w): 
        for y in range(h):
            zx = (-1.0 + 2.0 * x / w) * w / h
            zy = -1.0 + 2.0 * y / h
            
            c = complex(-0.05 + zx * zoom, 0.6805 + zy * zoom)
            z = complex(0, 0)
            
            for i in range(niter):
                if abs(z) > 2: 
                    break
                z = z**2 + c

            color = (i << 21) + (i << 10)  + i * 8
            pixels[y,x] = color
    return pixels

def display(width=1024, height=768, zoom=1.0, cmap='viridis'):

    pixels = mandelbrotSet(width, height, zoom=zoom)
    # print(pixels.tobytes())
    # Let us turn off the axes
    plt.axis('off')
    plt.imshow(pixels, cmap=cmap)
    plt.savefig("result.png")

import time
start = time.time()
# for i in range(100, 0, -1):
display(width=1024, height=768, zoom=9*10)


end = time.time()
print(end - start)