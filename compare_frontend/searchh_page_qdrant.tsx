
import { redirect } from "next/navigation"
import { useState } from 'react';
import { useRouter } from 'next/router';
import { Import, X } from 'lucide-react'
import Image from 'next/image'
import Link from 'next/link'
import { Separator } from "@/components/ui/separator";
import * as dotenv from 'dotenv'
dotenv.config()

const shopNames: { [key: number]: string } = {
  1: 'Screwfix',
  2: 'Tradepoint',
};

interface Product {
  pid: number;
  image_url: string;
  name: string;
  price: string;
  page_url: string;
  shop_id: number;
  category_name: string;
  // subcategory_name: string;
}


interface PageProps {
    searchParams: {
      [key: string]: string | string[] | undefined
    }
  }

  

  const Page = async ({ searchParams }: PageProps) => {
    const query = searchParams.query;
    if (Array.isArray(query) || !query) {
      return redirect('/');
    }
  
    try {
      
      const apiUrl = new URL('http://localhost:5000/search');
      apiUrl.searchParams.append('query', query.trim());
      
  
     
      const response = await fetch(apiUrl.toString());
      if (!response.ok) {
        
        throw new Error(`API call failed with status: ${response.status}`);
      }
  
      let results = await response.json();
      
      return (
        <ul className='py-4 divide-y divide-zinc-100 bg-white shadow-md rounded-b-md'>
        {results.products.slice(0, 100).map((product: Product) => (
          <Link key={product.pid} href={product.page_url} passHref>
            <li className='mx-auto py-4 px-8 flex space-x-4 cursor-pointer'>
              <div className='relative flex items-center bg-zinc-100 rounded-lg h-40 w-40'>
                <Image
                  src={product.image_url}
                  alt={product.name}
                  fill 
                  style={{ objectFit: 'cover' }} 
                  />
              </div>
              <div className='w-full flex-1 space-y-2 py-1'>
                <h1 className='text-lg font-medium text-gray-900'>
                  {product.name}
                </h1>
                <p className='prose prose-sm text-gray-500 line-clamp-3'>
                  {product.category_name} 
                  {/* &gt; {product.subcategory_name} */}
                </p>
                <p className='prose prose-sm text-gray-500 line-clamp-3'>
                  {shopNames[product.shop_id]}
                </p>
                <p className='text-base font-medium text-gray-900'>
                  £{product.price}
                </p>
              </div>
            </li>
            <Separator className=""/>
          </Link>
        ))}
      </ul>
      );
    } catch (error) {
      console.error('Error fetching products:', error);
       return (
        <div className='text-center py-4 bg-white shadow-md rounded-b-md'>
          <X className='mx-auto h-8 w-8 text-gray-400' />
          <h3 className='mt-2 text-sm font-semibold text-gray-900'>No results</h3>
          <p className='mt-1 text-sm mx-auto max-w-prose text-gray-500'>
            Sorry, we couldn't find any matches for{' '}
            <span className='text-green-600 font-medium'>{query}</span>.
          </p>
        </div>
      )
    }

  };
  

export default Page