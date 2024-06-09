'use client'

import React from 'react';
import * as NavigationMenu from '@radix-ui/react-navigation-menu';
import { CaretDownIcon } from '@radix-ui/react-icons';

interface Subcategory {
  id: number;
  name: string;
}

interface Category {
  id: number;
  name: string;
  subcategories: Subcategory[];
}

const categories: Category[] = [
  {
    id: 1,
    name: "Building Materials",
    subcategories: [
      { id: 1, name: "Cement & Aggregates" },
      { id: 2, name: "Insulation & Plasterboard" },
      { id: 3, name: "Timber & Sheet Materials" },
      { id: 4, name: "Roofing" }
    ]
  },
  {
    id: 2,
    name: "Decorating",
    subcategories: [
      { id: 5, name: "Brushes & Rollers" },
      { id: 6, name: "Wallpaper & Wall Coverings" },
      { id: 7, name: "Decorating Tools" },
      { id: 8, name: "Paint & Primer" }
    ]
  },
  {
    id: 3,
    name: "Plumbing & Heating",
    subcategories: [
      { id: 9, name: "Heating Controls" },
      { id: 10, name: "Plumbing Tools" },
      { id: 11, name: "Pipes & Fittings" },
      { id: 12, name: "Boilers & Radiators" }
    ]
  },
  {
    id: 4,
    name: "Electrical & Lighting",
    subcategories: [
      { id: 13, name: "Switches & Sockets" },
      { id: 14, name: "Light Fixtures & Bulbs" },
      { id: 15, name: "Cables & Accessories" },
      { id: 16, name: "Electrical Tools" },
      { id: 36, name: "Security" }
    ]
  },
  {
    id: 5,
    name: "Kitchens & Bathrooms",
    subcategories: [
      { id: 17, name: "Accessories" },
      { id: 18, name: "Taps & Sinks" },
      { id: 19, name: "Kitchen Cabinets" },
      { id: 20, name: "Bathroom Suites" },
      { id: 34, name: "Assisted Living" },
      { id: 35, name: "Commercial Bathrooms" }
    ]
  },
  {
    id: 6,
    name: "Hardware & Tools",
    subcategories: [
      { id: 21, name: "Nails" },
      { id: 23, name: "Screws" },
      { id: 24, name: "Bolts & Nuts" },
      { id: 22, name: "Fixings & Fastners" }
    ]
  },
  {
    id: 7,
    name: "Flooring & Tiling",
    subcategories: [
      { id: 25, name: "Flooring" },
      { id: 26, name: "Stairs & Railings" },
      { id: 27, name: "Tiles" },
      { id: 28, name: "Doors & Windows" },
      { id: 37, name: "Doors & Windows Accessories" }
    ]
  },
  {
    id: 8,
    name: "Paints & Adhesives",
    subcategories: [
      { id: 29, name: "Sealants" },
      { id: 30, name: "Tapes" },
      { id: 31, name: "Adhesives" },
      { id: 32, name: "Fillers" }
    ]
  },
  {
    id: 9,
    name: "Garden & Outdoor",
    subcategories: [
      { id: 33, name: "Garden" }
    ]
  }
];


const Categories: React.FC = (): JSX.Element => {
  return (
    <NavigationMenu.Root className="relative z-[1] flex w-screen justify-center">
      <NavigationMenu.List className="center shadow-blackA4 m-0 flex list-none rounded-[6px] bg-white p-1 shadow-[0_1px_5px]">
        {categories.map((category) => (
          <NavigationMenu.Item key={category.id}>
            <NavigationMenu.Trigger className="text-violet11 hover:bg-violet3 focus:shadow-violet7 group flex select-none items-center justify-between gap-[2px] rounded-[4px] px-3 py-2 text-[15px] font-medium leading-none outline-none focus:shadow-[0_0_0_2px]">
              {category.name}
              <CaretDownIcon
                className="text-violet10 relative top-[1px] transition-transform duration-[250] ease-in group-data-[state=open]:-rotate-180"
                aria-hidden
              />
            </NavigationMenu.Trigger>
            <NavigationMenu.Content className="data-[motion=from-start]:animate-enterFromLeft data-[motion=from-end]:animate-enterFromRight data-[motion=to-start]:animate-exitToLeft data-[motion=to-end]:animate-exitToRight absolute top-0 left-0 w-full sm:w-auto">
              <NavigationMenu.List className="one m-0 grid list-none gap-x-[10px] p-[22px] sm:w-[500px] sm:grid-cols-[0.75fr_1fr]">
                {category.subcategories.map((sub) => (
                  <NavigationMenu.Item className="row-span-3 grid" key={sub.id}>
                    <NavigationMenu.Link asChild>
                      <a href="#" className="text-indigo-700 hover:text-indigo-900 block select-none rounded-[6px] p-3 text-[15px] leading-none no-underline">
                        {sub.name}
                      </a>
                    </NavigationMenu.Link>
                  </NavigationMenu.Item>
                ))}
              </NavigationMenu.List>
            </NavigationMenu.Content>
          </NavigationMenu.Item>
        ))}
      </NavigationMenu.List>
    </NavigationMenu.Root>
  );
};

export default Categories;